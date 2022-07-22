// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrate

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func MigrateCommit(ctx context.Context, r ref.DoltRef, cm *doltdb.Commit, new *doltdb.DoltDB, prog Progress) error {
	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	mRoot, err := MigrateRoot(ctx, root, new)
	if err != nil {
		return err
	}
	_, addr, err := new.WriteRootValue(ctx, mRoot)
	if err != nil {
		return err
	}
	value, err := new.ValueReadWriter().ReadValue(ctx, addr)
	if err != nil {
		return err
	}

	opts, err := MigrateCommitOptions(ctx, cm, prog)
	if err != nil {
		return err
	}

	migratedCm, err := new.CommitValue(ctx, r, value, opts)
	if err != nil {
		return err
	}

	// update progress
	oldHash, err := cm.HashOf()
	if err != nil {
		return err
	}
	newHash, err := migratedCm.HashOf()
	if err != nil {
		return err
	}

	return prog.Put(ctx, oldHash, newHash)
}

func MigrateCommitOptions(ctx context.Context, oldCm *doltdb.Commit, prog Progress) (datas.CommitOptions, error) {
	parents, err := oldCm.ParentHashes(ctx)
	if err != nil {
		return datas.CommitOptions{}, err
	}

	for i := range parents {
		migrated, err := prog.Get(ctx, parents[i])
		if err != nil {
			return datas.CommitOptions{}, err
		}
		parents[i] = migrated
	}

	meta, err := oldCm.GetCommitMeta(ctx)
	if err != nil {
		return datas.CommitOptions{}, err
	}

	return datas.CommitOptions{
		Parents: parents,
		Meta:    meta,
	}, nil
}

func MigrateRoot(ctx context.Context, root *doltdb.RootValue, new *doltdb.DoltDB) (*doltdb.RootValue, error) {
	migrated, err := doltdb.EmptyRootValue(ctx, new.ValueReadWriter(), new.NodeStore())
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	migrated, err = migrated.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, err
	}

	err = root.IterTables(ctx, func(name string, tbl *doltdb.Table, _ schema.Schema) (bool, error) {
		mtbl, err := MigrateTable(ctx, tbl, new)
		if err != nil {
			return true, err
		}

		migrated, err = migrated.PutTable(ctx, name, mtbl)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return migrated, nil
}

func MigrateTable(ctx context.Context, table *doltdb.Table, new *doltdb.DoltDB) (*doltdb.Table, error) {
	rows, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	err = MigrateNomsMap(ctx, rows, table.ValueReadWriter(), new.ValueReadWriter())
	if err != nil {
		return nil, err
	}

	ai, err := table.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	autoInc := types.Uint(ai)

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	oldSet, err := table.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	newSet, err := MigrateIndexSet(ctx, sch, oldSet, table.ValueReadWriter(), new)
	if err != nil {
		return nil, err
	}

	return doltdb.NewTable(ctx, new.ValueReadWriter(), new.NodeStore(), sch, rows, newSet, autoInc)
}

func MigrateIndexSet(ctx context.Context, sch schema.Schema, oldSet durable.IndexSet, old types.ValueReadWriter, new *doltdb.DoltDB) (durable.IndexSet, error) {
	newSet := durable.NewIndexSet(ctx, new.ValueReadWriter(), new.NodeStore())
	for _, def := range sch.Indexes().AllIndexes() {
		idx, err := oldSet.GetIndex(ctx, sch, def.Name())
		if err != nil {
			return nil, err
		}
		if err = MigrateNomsMap(ctx, idx, old, new.ValueReadWriter()); err != nil {
			return nil, err
		}

		newSet, err = newSet.PutIndex(ctx, def.Name(), idx)
		if err != nil {
			return nil, err
		}
	}
	return newSet, nil
}

func MigrateNomsMap(ctx context.Context, idx durable.Index, old, new types.ValueReadWriter) error {
	m := durable.NomsMapFromIndex(idx)
	_, _, err := types.VisitMapLevelOrder(m, func(h hash.Hash) (int64, error) {
		v, err := old.ReadValue(ctx, h)
		if err != nil {
			return 0, err
		}
		_, err = new.WriteValue(ctx, v)
		return 0, err
	})
	return err
}
