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
	"fmt"
	"io"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var (
	flushRef = ref.NewInternalRef("migration-flush")
)

func migrateWorkingSet(ctx context.Context, wsRef ref.WorkingSetRef, old, new *doltdb.DoltDB, prog Progress) error {
	oldWs, err := old.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	wr, err := migrateRoot(ctx, oldWs.WorkingRoot(), new)
	if err != nil {
		return err
	}

	sr, err := migrateRoot(ctx, oldWs.StagedRoot(), new)
	if err != nil {
		return err
	}

	newWs := doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(wr).WithStagedRoot(sr)

	return new.UpdateWorkingSet(ctx, wsRef, newWs, hash.Hash{}, oldWs.Meta())
}

func migrateCommit(ctx context.Context, cm *doltdb.Commit, new *doltdb.DoltDB, prog Progress) error {
	oldHash, err := cm.HashOf()
	if err != nil {
		return err
	}

	ok, err := prog.Has(ctx, oldHash)
	if err != nil {
		return err
	} else if ok {
		return nil
	}

	if cm.NumParents() == 0 {
		return migrateInitCommit(ctx, cm, new, prog)
	}

	prog.Log(ctx, "migrating commit %s", oldHash.String())

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	mRoot, err := migrateRoot(ctx, root, new)
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

	opts, err := migrateCommitOptions(ctx, cm, prog)
	if err != nil {
		return err
	}

	migratedCm, err := new.CommitDangling(ctx, value, opts)
	if err != nil {
		return err
	}

	// update progress
	newHash, err := migratedCm.HashOf()
	if err != nil {
		return err
	}
	if err = prog.Put(ctx, oldHash, newHash); err != nil {
		return err
	}

	// flush ChunkStore
	return new.SetHead(ctx, flushRef, newHash)
}

func migrateInitCommit(ctx context.Context, cm *doltdb.Commit, new *doltdb.DoltDB, prog Progress) error {
	oldHash, err := cm.HashOf()
	if err != nil {
		return err
	}

	rv, err := doltdb.EmptyRootValue(ctx, new.ValueReadWriter(), new.NodeStore())
	if err != nil {
		return err
	}
	nv := doltdb.HackNomsValuesFromRootValues(rv)

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return err
	}
	datasDB := doltdb.HackDatasDatabaseFromDoltDB(new)

	creation := ref.NewInternalRef(doltdb.CreationBranch)
	ds, err := datasDB.GetDataset(ctx, creation.String())
	if err != nil {
		return err
	}
	ds, err = datasDB.Commit(ctx, ds, nv, datas.CommitOptions{Meta: meta})
	if err != nil {
		return err
	}

	newCm, err := new.ResolveCommitRef(ctx, creation)
	if err != nil {
		return err
	}
	newHash, err := newCm.HashOf()
	if err != nil {
		return err
	}

	return prog.Put(ctx, oldHash, newHash)
}

func migrateCommitOptions(ctx context.Context, oldCm *doltdb.Commit, prog Progress) (datas.CommitOptions, error) {
	parents, err := oldCm.ParentHashes(ctx)
	if err != nil {
		return datas.CommitOptions{}, err
	}
	if len(parents) == 0 {
		panic("expected non-zero parents list")
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

func migrateRoot(ctx context.Context, root *doltdb.RootValue, new *doltdb.DoltDB) (*doltdb.RootValue, error) {
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
		mtbl, err := migrateTable(ctx, name, tbl, new)
		if err != nil {
			return true, err
		}

		ok, err := tbl.HasConflicts(ctx)
		if err != nil {
			return true, err
		} else if ok {
			return true, fmt.Errorf("cannot migrate table with conflicts (%s)", name)
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

	if err = validateRootValue(ctx, root, migrated); err != nil {
		return nil, err
	}

	return migrated, nil
}

func migrateTable(ctx context.Context, name string, table *doltdb.Table, new *doltdb.DoltDB) (*doltdb.Table, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// maybe patch dolt_schemas, dolt docs
	if doltdb.HasDoltPrefix(name) {
		if sch, err = patchMigrateSchema(ctx, sch); err != nil {
			return nil, err
		}
	}

	if err = validateSchema(sch); err != nil {
		return nil, err
	}

	rows, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	newRows, err := migrateIndex(ctx, sch, rows, table.ValueReadWriter(), new.NodeStore())
	if err != nil {
		return nil, err
	}

	oldSet, err := table.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	newSet, err := migrateIndexSet(ctx, sch, oldSet, table.ValueReadWriter(), new)
	if err != nil {
		return nil, err
	}

	ai, err := table.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	autoInc := types.Uint(ai)

	return doltdb.NewTable(ctx, new.ValueReadWriter(), new.NodeStore(), sch, newRows, newSet, autoInc)
}

// patchMigrateSchema attempts to correct irregularities in existing schemas
func patchMigrateSchema(ctx context.Context, existing schema.Schema) (schema.Schema, error) {
	cols := existing.GetAllCols().GetColumns()

	var patched bool
	for i, c := range cols {
		qt := c.TypeInfo.ToSqlType().Type()
		// dolt_schemas and dolt_docs previously written with TEXT columns
		if qt == query.Type_TEXT && c.Kind == types.StringKind {
			cols[i] = schema.NewColumn(c.Name, c.Tag, c.Kind, c.IsPartOfPK, c.Constraints...)
			patched = true
		}
	}
	if !patched {
		return existing, nil
	}

	return schema.SchemaFromCols(schema.NewColCollection(cols...))
}

func migrateIndexSet(ctx context.Context, sch schema.Schema, oldSet durable.IndexSet, old types.ValueReadWriter, new *doltdb.DoltDB) (durable.IndexSet, error) {
	newSet := durable.NewIndexSet(ctx, new.ValueReadWriter(), new.NodeStore())
	for _, def := range sch.Indexes().AllIndexes() {
		idx, err := oldSet.GetIndex(ctx, sch, def.Name())
		if err != nil {
			return nil, err
		}

		newIdx, err := migrateIndex(ctx, def.Schema(), idx, old, new.NodeStore())
		if err != nil {
			return nil, err
		}

		newSet, err = newSet.PutIndex(ctx, def.Name(), newIdx)
		if err != nil {
			return nil, err
		}
	}
	return newSet, nil
}

func migrateIndex(ctx context.Context, sch schema.Schema, idx durable.Index, old types.ValueReadWriter, ns tree.NodeStore) (durable.Index, error) {
	eg, ctx := errgroup.WithContext(ctx)
	reader := make(chan types.Tuple, 256)
	writer := make(chan val.Tuple, 256)

	kt, vt := tupleTranslatorsFromSchema(sch, ns)
	kd := kt.builder.Desc
	vd := vt.builder.Desc

	var oldMap = durable.NomsMapFromIndex(idx)
	var newMap prolly.Map

	// read old noms map
	eg.Go(func() error {
		defer close(reader)
		return readNomsMap(ctx, oldMap, reader)
	})

	// translate noms tuples to prolly tuples
	eg.Go(func() error {
		defer close(writer)
		return translateTuples(ctx, kt, vt, reader, writer)
	})

	// write tuples in new prolly map
	eg.Go(func() (err error) {
		newMap, err = writeProllyMap(ctx, ns, kd, vd, writer)
		return
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return durable.IndexFromProllyMap(newMap), nil
}

func readNomsMap(ctx context.Context, m types.Map, reader chan<- types.Tuple) error {
	return m.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		select {
		case reader <- key.(types.Tuple):
		case _ = <-ctx.Done():
			stop = true
			return
		}
		select {
		case reader <- value.(types.Tuple):
		case _ = <-ctx.Done():
			stop = true
			return
		}
		return
	})
}

func translateTuples(ctx context.Context, kt, vt translator, reader <-chan types.Tuple, writer chan<- val.Tuple) error {
	for {
		var (
			oldKey types.Tuple
			oldVal types.Tuple
			ok     bool
		)

		select {
		case oldKey, ok = <-reader:
			if !ok {
				return nil // done
			}
		case _ = <-ctx.Done():
			return nil
		}

		newKey, err := kt.TranslateTuple(ctx, oldKey)
		if err != nil {
			return err
		}

		select {
		case writer <- newKey:
		case _ = <-ctx.Done():
			return nil
		}

		select {
		case oldVal, ok = <-reader:
			assertTrue(ok)
		case _ = <-ctx.Done():
			return nil
		}

		newVal, err := vt.TranslateTuple(ctx, oldVal)
		if err != nil {
			return err
		}

		select {
		case writer <- newVal:
		case _ = <-ctx.Done():
			return nil
		}
	}
}

func writeProllyMap(ctx context.Context, ns tree.NodeStore, kd, vd val.TupleDesc, writer <-chan val.Tuple) (prolly.Map, error) {
	return prolly.NewMapFromProvider(ctx, ns, kd, vd, channelProvider{tuples: writer})
}

type channelProvider struct {
	tuples <-chan val.Tuple
}

var _ prolly.TupleProvider = channelProvider{}

func (p channelProvider) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	var (
		k, v val.Tuple
		ok   bool
	)

	select {
	case k, ok = <-p.tuples:
		if !ok {
			return nil, nil, io.EOF // done
		}
	case _ = <-ctx.Done():
		return nil, nil, nil
	}

	select {
	case v, ok = <-p.tuples:
		assertTrue(ok)
	case _ = <-ctx.Done():
		return nil, nil, nil
	}
	return k, v, nil
}
