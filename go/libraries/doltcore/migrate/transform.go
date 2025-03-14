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

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/chunks"
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

func migrateWorkingSet(ctx context.Context, menv Environment, brRef ref.BranchRef, wsRef ref.WorkingSetRef, old, new *doltdb.DoltDB) error {
	oldHead, err := old.ResolveCommitRef(ctx, brRef)
	if err != nil {
		return err
	}
	oldHeadRoot, err := oldHead.GetRootValue(ctx)
	if err != nil {
		return err
	}

	oldWs, err := old.ResolveWorkingSet(ctx, wsRef)
	if err == doltdb.ErrWorkingSetNotFound {
		// If a branch was created prior to dolt version 0.26.10, no working set will exist for it.
		// In this case, we will pretend it exists with the same root as the head commit.
		oldWs = doltdb.EmptyWorkingSet(wsRef)
		oldWs = oldWs.WithWorkingRoot(oldHeadRoot).WithStagedRoot(oldHeadRoot)
	} else if err != nil {
		return err
	}

	newHead, err := new.ResolveCommitRef(ctx, brRef)
	if err != nil {
		return err
	}
	newHeadRoot, err := newHead.GetRootValue(ctx)
	if err != nil {
		return err
	}

	wr, err := migrateRoot(ctx, menv, oldHeadRoot, oldWs.WorkingRoot(), newHeadRoot)
	if err != nil {
		return err
	}

	sr, err := migrateRoot(ctx, menv, oldHeadRoot, oldWs.StagedRoot(), newHeadRoot)
	if err != nil {
		return err
	}

	err = validateRootValue(ctx, oldHeadRoot, oldWs.WorkingRoot(), wr)
	if err != nil {
		return err
	}

	err = validateRootValue(ctx, oldHeadRoot, oldWs.StagedRoot(), sr)
	if err != nil {
		return err
	}

	newWs := doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(wr).WithStagedRoot(sr)

	return new.UpdateWorkingSet(ctx, wsRef, newWs, hash.Hash{}, oldWs.Meta(), nil)
}

func migrateCommit(ctx context.Context, menv Environment, oldCm *doltdb.Commit, new *doltdb.DoltDB, prog *progress) error {
	oldHash, err := oldCm.HashOf()
	if err != nil {
		return err
	}

	ok, err := prog.Has(ctx, oldHash)
	if err != nil {
		return err
	} else if ok {
		return nil
	}

	if oldCm.NumParents() == 0 {
		return migrateInitCommit(ctx, oldCm, new, prog)
	}

	hs := oldHash.String()
	prog.Log(ctx, "migrating commit %s", hs)

	oldRoot, err := oldCm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	optCmt, err := oldCm.GetParent(ctx, 0)
	if err != nil {
		return err
	}
	oldParentCm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	oldParentRoot, err := oldParentCm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	oph, err := oldParentCm.HashOf()
	if err != nil {
		return err
	}
	ok, err = prog.Has(ctx, oph)
	if err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("cannot find commit mapping for Commit (%s)", oph.String())
	}

	newParentAddr, err := prog.Get(ctx, oph)
	if err != nil {
		return err
	}
	optCmt, err = new.ReadCommit(ctx, newParentAddr)
	if err != nil {
		return err
	}
	newParentCm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	newParentRoot, err := newParentCm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	mRoot, err := migrateRoot(ctx, menv, oldParentRoot, oldRoot, newParentRoot)
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

	opts, err := migrateCommitOptions(ctx, oldCm, prog)
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
	if err = new.SetHead(ctx, flushRef, newHash); err != nil {
		return err
	}
	err = new.ShallowGC(ctx)
	if err != nil && err != chunks.ErrUnsupportedOperation {
		return err
	}

	// validate root after we flush the ChunkStore to facilitate
	// investigating failed migrations
	if err = validateRootValue(ctx, oldParentRoot, oldRoot, mRoot); err != nil {
		return err
	}

	return nil
}

func migrateInitCommit(ctx context.Context, cm *doltdb.Commit, new *doltdb.DoltDB, prog *progress) error {
	oldHash, err := cm.HashOf()
	if err != nil {
		return err
	}

	rv, err := doltdb.EmptyRootValue(ctx, new.ValueReadWriter(), new.NodeStore())
	if err != nil {
		return err
	}

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
	ds, err = datasDB.Commit(ctx, ds, rv.NomsValue(), datas.CommitOptions{Meta: meta})
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

func migrateCommitOptions(ctx context.Context, oldCm *doltdb.Commit, prog *progress) (datas.CommitOptions, error) {
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

func migrateRoot(ctx context.Context, menv Environment, oldParent, oldRoot, newParent doltdb.RootValue) (doltdb.RootValue, error) {
	migrated := newParent

	fkc, err := oldRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	migrated, err = migrated.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return nil, err
	}

	removedTables, err := getRemovedTableNames(ctx, oldParent, oldRoot)
	if err != nil {
		return nil, err
	}

	migrated, err = migrated.RemoveTables(ctx, true, false, doltdb.ToTableNames(removedTables, doltdb.DefaultSchemaName)...)
	if err != nil {
		return nil, err
	}

	err = oldRoot.IterTables(ctx, func(name doltdb.TableName, oldTbl *doltdb.Table, sch schema.Schema) (bool, error) {
		ok, err := oldTbl.HasConflicts(ctx)
		if err != nil {
			return true, err
		} else if ok && !menv.DropConflicts {
			return true, fmt.Errorf("cannot migrate table with conflicts (%s)", name)
		}

		// TODO: schema names
		newSch, err := migrateSchema(ctx, name.Name, sch)
		if err != nil {
			return true, err
		}
		if err = validateSchema(newSch); err != nil {
			return true, err
		}

		// if there was a schema change in this commit,
		// diff against an empty table and rewrite everything
		var parentSch schema.Schema

		oldParentTbl, ok, err := oldParent.GetTable(ctx, name)
		if err != nil {
			return true, err
		}
		if ok {
			parentSch, err = oldParentTbl.GetSchema(ctx)
			if err != nil {
				return true, err
			}
		}
		if !ok || !schema.SchemasAreEqual(sch, parentSch) {
			// provide empty table to diff against
			oldParentTbl, err = doltdb.NewEmptyTable(ctx, oldParent.VRW(), oldParent.NodeStore(), sch)
			if err != nil {
				return true, err
			}
		}

		newParentTbl, ok, err := newParent.GetTable(ctx, name)
		if err != nil {
			return true, err
		}
		if !ok || !schema.SchemasAreEqual(sch, parentSch) {
			// provide empty table to diff against
			newParentTbl, err = doltdb.NewEmptyTable(ctx, newParent.VRW(), newParent.NodeStore(), newSch)
			if err != nil {
				return true, err
			}
		}

		mtbl, err := migrateTable(ctx, newSch, oldParentTbl, oldTbl, newParentTbl)
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

// renames also get returned here
func getRemovedTableNames(ctx context.Context, prev, curr doltdb.RootValue) ([]string, error) {
	prevNames, err := prev.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	tblNameSet := set.NewStrSet(prevNames)
	currNames, err := curr.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	tblNameSet.Remove(currNames...)
	return tblNameSet.AsSlice(), nil
}

func migrateTable(ctx context.Context, newSch schema.Schema, oldParentTbl, oldTbl, newParentTbl *doltdb.Table) (*doltdb.Table, error) {
	idx, err := oldParentTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	oldParentRows := durable.NomsMapFromIndex(idx)

	idx, err = oldTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	oldRows := durable.NomsMapFromIndex(idx)

	idx, err = newParentTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	newParentRows, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return nil, err
	}

	oldParentSet, err := oldParentTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	oldSet, err := oldTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	newParentSet, err := newParentTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	var newRows durable.Index
	var newSet durable.IndexSet
	originalCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		var merr error
		newRows, merr = migrateIndex(ctx, newSch, oldParentRows, oldRows, newParentRows, newParentTbl.NodeStore())
		return merr
	})

	vrw, ns := newParentTbl.ValueReadWriter(), newParentTbl.NodeStore()
	eg.Go(func() error {
		var merr error
		newSet, merr = migrateIndexSet(ctx, newSch, oldParentSet, oldSet, newParentSet, vrw, ns)
		return merr
	})

	if err = eg.Wait(); err != nil {
		return nil, err
	}

	ai, err := oldTbl.GetAutoIncrementValue(originalCtx)
	if err != nil {
		return nil, err
	}
	autoInc := types.Uint(ai)

	return doltdb.NewTable(originalCtx, vrw, ns, newSch, newRows, newSet, autoInc)
}

func migrateSchema(ctx context.Context, tableName string, existing schema.Schema) (schema.Schema, error) {
	// dolt_schemas and dolt_docs previously included columns with
	// SQL type TEXT, but NomsKind of StringKind
	if doltdb.HasDoltPrefix(tableName) {
		var patched bool
		cols := existing.GetAllCols().GetColumns()
		for i, c := range cols {
			qt := c.TypeInfo.ToSqlType().Type()
			if qt == query.Type_TEXT && c.Kind == types.StringKind {
				// NewColumn picks SQL type from NomsKind, converting this TEXT column to VARCHAR
				cols[i] = schema.NewColumn(c.Name, c.Tag, c.Kind, c.IsPartOfPK, c.Constraints...)
				patched = true
			}
		}
		if patched {
			allCols := schema.NewColCollection(cols...)
			schema.NewIndexCollection(allCols, existing.GetPKCols())
			return schema.NewSchema(
				allCols,
				existing.GetPkOrdinals(),
				existing.GetCollation(),
				existing.Indexes(),
				existing.Checks(),
			)
		}
		return existing, nil
	}

	// Blob types cannot be index keys in the new format:
	// substitute VARCHAR(max) for TEXT, VARBINARY(max) for BLOB
	// TODO: print warning to users
	var patched bool
	tags := schema.GetKeyColumnTags(existing)
	cols := existing.GetAllCols().GetColumns()
	for i, c := range cols {
		if tags.Contains(c.Tag) {
			var err error
			switch c.TypeInfo.ToSqlType().Type() {
			case query.Type_TEXT:
				patched = true
				info := typeinfo.StringDefaultType
				cols[i], err = schema.NewColumnWithTypeInfo(c.Name, c.Tag, info, c.IsPartOfPK, c.Default, c.AutoIncrement, c.Comment, c.Constraints...)
			case query.Type_BLOB:
				patched = true
				info := typeinfo.VarbinaryDefaultType
				cols[i], err = schema.NewColumnWithTypeInfo(c.Name, c.Tag, info, c.IsPartOfPK, c.Default, c.AutoIncrement, c.Comment, c.Constraints...)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// String types are sorted using a binary collation in __LD_1__
	// force-set collation to utf8mb4_0900_bin to match the order
	for i, c := range cols {
		st, ok := c.TypeInfo.ToSqlType().(sql.StringType)
		if !ok {
			continue
		}
		patched = true

		var err error
		switch st.Type() {
		case query.Type_CHAR, query.Type_VARCHAR, query.Type_TEXT:
			st, err = gmstypes.CreateString(st.Type(), st.Length(), sql.Collation_utf8mb4_0900_bin)
		case query.Type_BINARY, query.Type_VARBINARY, query.Type_BLOB:
			st, err = gmstypes.CreateString(st.Type(), st.Length(), sql.Collation_binary)
		}
		if err != nil {
			return nil, err
		}

		info, err := typeinfo.FromSqlType(st)
		if err != nil {
			return nil, err
		}

		cols[i], err = schema.NewColumnWithTypeInfo(c.Name, c.Tag, info, c.IsPartOfPK, c.Default, c.AutoIncrement, c.Comment, c.Constraints...)
		if err != nil {
			return nil, err
		}
	}

	if !patched {
		return existing, nil
	}

	sch, err := schema.NewSchema(
		schema.NewColCollection(cols...),
		existing.GetPkOrdinals(),
		existing.GetCollation(),
		existing.Indexes(),
		existing.Checks(),
	)
	if err != nil {
		return nil, err
	}

	return sch, nil
}

func migrateIndexSet(
	ctx context.Context,
	sch schema.Schema,
	oldParentSet, oldSet, newParentSet durable.IndexSet,
	vrw types.ValueReadWriter, ns tree.NodeStore,
) (durable.IndexSet, error) {
	newSet, err := durable.NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}
	for _, def := range sch.Indexes().AllIndexes() {
		idx, err := oldParentSet.GetIndex(ctx, sch, nil, def.Name())
		if err != nil {
			return nil, err
		}
		oldParent := durable.NomsMapFromIndex(idx)

		idx, err = oldSet.GetIndex(ctx, sch, nil, def.Name())
		if err != nil {
			return nil, err
		}
		old := durable.NomsMapFromIndex(idx)

		idx, err = newParentSet.GetIndex(ctx, sch, nil, def.Name())
		if err != nil {
			return nil, err
		}
		newParent, err := durable.ProllyMapFromIndex(idx)
		if err != nil {
			return nil, err
		}

		newIdx, err := migrateIndex(ctx, def.Schema(), oldParent, old, newParent, ns)
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

func migrateIndex(
	ctx context.Context,
	sch schema.Schema,
	oldParent, oldMap types.Map,
	newParent prolly.Map,
	ns tree.NodeStore,
) (durable.Index, error) {

	eg, ctx := errgroup.WithContext(ctx)
	differ := make(chan types.ValueChanged, 256)
	writer := make(chan val.Tuple, 256)

	kt, vt := tupleTranslatorsFromSchema(sch, ns)

	// read old noms map
	eg.Go(func() error {
		defer close(differ)
		return oldMap.Diff(ctx, oldParent, differ)
	})

	// translate noms tuples to prolly tuples
	eg.Go(func() error {
		defer close(writer)
		return translateTuples(ctx, kt, vt, differ, writer)
	})

	var newMap prolly.Map
	// write tuples in new prolly map
	eg.Go(func() (err error) {
		newMap, err = writeProllyMap(ctx, newParent, writer)
		return
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return durable.IndexFromProllyMap(newMap), nil
}

func translateTuples(ctx context.Context, kt, vt translator, differ <-chan types.ValueChanged, writer chan<- val.Tuple) error {
	for {
		var (
			diff   types.ValueChanged
			newKey val.Tuple
			newVal val.Tuple
			ok     bool
			err    error
		)

		select {
		case diff, ok = <-differ:
			if !ok {
				return nil // done
			}
		case _ = <-ctx.Done():
			return ctx.Err()
		}

		switch diff.ChangeType {
		case types.DiffChangeAdded:
			fallthrough

		case types.DiffChangeModified:
			newVal, err = vt.TranslateTuple(ctx, diff.NewValue.(types.Tuple))
			if err != nil {
				return err
			}
			fallthrough

		case types.DiffChangeRemoved:
			newKey, err = kt.TranslateTuple(ctx, diff.Key.(types.Tuple))
			if err != nil {
				return err
			}
		}

		select {
		case writer <- newKey:
		case _ = <-ctx.Done():
			return ctx.Err()
		}

		select {
		case writer <- newVal:
		case _ = <-ctx.Done():
			return ctx.Err()
		}
	}
}

func writeProllyMap(ctx context.Context, prev prolly.Map, writer <-chan val.Tuple) (m prolly.Map, err error) {
	var (
		k, v val.Tuple
		ok   bool
	)

	mut := prev.Mutate()
	for {
		select {
		case k, ok = <-writer:
			if !ok {
				m, err = mut.Map(ctx)
				return // done
			}
		case <-ctx.Done():
			return
		}

		select {
		case v, ok = <-writer:
			assertTrue(ok)
		case <-ctx.Done():
			return
		}
		if err = mut.Put(ctx, k, v); err != nil {
			return
		}
	}
}
