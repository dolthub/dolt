// Copyright 2026 Dolthub, Inc.
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

package merge

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	errorkinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrRekeyedAncestorCollision = errorkinds.NewKind("error: cannot merge table %s: both sides changed its primary key, and two rows of the common ancestor have the same new primary key")
var ErrRekeyedAncestorNullKey = errorkinds.NewKind("error: cannot merge table %s: both sides changed its primary key, and a row of the common ancestor has a NULL in the new primary key")

// rekeyAncestorTables returns |ancRoot| with each table of |tblNames| whose primary key was changed the same way on
// both sides of the merge rewritten to that new primary key. The merge diffs each side against the ancestor by
// primary key, so an ancestor keyed differently from both sides cannot otherwise be merged against. Tables that
// cannot be rewritten are left alone, and SchemaMerge rejects them as before.
func rekeyAncestorTables(ctx *sql.Context, ourRoot, theirRoot, ancRoot doltdb.RootValue, tblNames []doltdb.TableName) (doltdb.RootValue, error) {
	for _, name := range tblNames {
		ancTbl, ok, err := ancRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		ourTbl, ok, err := ourRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		theirTbl, ok, err := theirRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		ancSch, err := ancTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		ourSch, err := ourTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		theirSch, err := theirTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		if !needsRekeyedAncestor(ourSch, theirSch, ancSch) {
			continue
		}
		rekeyed, ok, err := RekeyTable(ctx, name, ancTbl, ancSch, ourSch)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if ancRoot, err = ancRoot.PutTable(ctx, name, rekeyed); err != nil {
			return nil, err
		}
	}
	return ancRoot, nil
}

// needsRekeyedAncestor returns whether both sides agree on a primary key that differs from the ancestor's.
func needsRekeyedAncestor(ourSch, theirSch, ancSch schema.Schema) bool {
	if schema.IsKeyless(ourSch) || schema.IsKeyless(ancSch) {
		return false
	}
	return schema.ComparePrimaryKeys(ourSch, theirSch) == schema.PrimaryKeysSameTags &&
		schema.ComparePrimaryKeys(ourSch, ancSch) != schema.PrimaryKeysSameTags
}

// RekeyTable returns |tbl|, whose schema is |sch|, rewritten to the primary key of |target|: the same columns and
// rows, keyed and indexed by the columns of |target|'s primary key. It returns false when |sch| lacks one of those
// columns, or holds it with another type or encoding.
func RekeyTable(ctx *sql.Context, name doltdb.TableName, tbl *doltdb.Table, sch, target schema.Schema) (*doltdb.Table, bool, error) {
	newSch, ok, err := schema.RekeyedSchema(sch, target)
	if err != nil || !ok {
		return nil, false, err
	}
	if hasArtifacts, err := tableHasArtifacts(ctx, tbl); err != nil || hasArtifacts {
		return nil, false, err
	}

	vrw, ns := tbl.ValueReadWriter(), tbl.NodeStore()
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	keySrc, ok := fieldSources(sch, newSch.GetPKCols().GetColumns(), oldKD, oldVD, newKD)
	if !ok {
		return nil, false, nil
	}
	valSrc, ok := fieldSources(sch, storedCols(newSch.GetNonPKCols()), oldKD, oldVD, newVD)
	if !ok {
		return nil, false, nil
	}

	rd, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}
	oldRows, err := durable.ProllyMapFromIndex(rd)
	if err != nil {
		return nil, false, err
	}
	empty, err := durable.NewEmptyPrimaryIndex(ctx, vrw, ns, newSch)
	if err != nil {
		return nil, false, err
	}
	emptyRows, err := durable.ProllyMapFromIndex(empty)
	if err != nil {
		return nil, false, err
	}
	mut := emptyRows.Mutate()

	pool := oldRows.Pool()
	kb, vb := val.NewTupleBuilder(newKD, ns), val.NewTupleBuilder(newVD, ns)
	iter, err := oldRows.IterAll(ctx)
	if err != nil {
		return nil, false, err
	}
	for {
		k, v, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, false, err
		}
		for i, src := range keySrc {
			field := src.get(k, v)
			if field == nil {
				return nil, false, ErrRekeyedAncestorNullKey.New(name)
			}
			kb.PutRaw(i, field)
		}
		for i, src := range valSrc {
			vb.PutRaw(i, src.get(k, v))
		}
		nk, err := kb.Build(ctx, pool)
		if err != nil {
			return nil, false, err
		}
		nv, err := vb.Build(ctx, pool)
		if err != nil {
			return nil, false, err
		}
		if dup, err := mut.Has(ctx, nk); err != nil {
			return nil, false, err
		} else if dup {
			return nil, false, ErrRekeyedAncestorCollision.New(name)
		}
		if err = mut.Put(ctx, nk, nv); err != nil {
			return nil, false, err
		}
	}
	newRows, err := mut.Map(ctx)
	if err != nil {
		return nil, false, err
	}

	indexes, err := durable.NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, false, err
	}
	for _, index := range newSch.Indexes().AllIndexes() {
		built, err := creation.BuildProllyIndexExternal(ctx, vrw, ns, newSch, name.Name, index, newRows, nil, nil)
		if err != nil {
			return nil, false, err
		}
		if indexes, err = indexes.PutIndex(ctx, index.Name(), built); err != nil {
			return nil, false, err
		}
	}

	var autoInc types.Value
	if schema.HasAutoIncrement(newSch) {
		v, err := tbl.GetAutoIncrementValue(ctx)
		if err != nil {
			return nil, false, err
		}
		autoInc = types.Uint(v)
	}
	rekeyed, err := doltdb.NewTable(ctx, vrw, ns, newSch, durable.IndexFromProllyMap(newRows), indexes, autoInc)
	if err != nil {
		return nil, false, err
	}
	return rekeyed, true, nil
}

// fieldSource is where a field of a rekeyed tuple comes from: a field of the old key or of the old value.
type fieldSource struct {
	fromKey bool
	idx     int
	desc    *val.TupleDesc
}

func (s fieldSource) get(k, v val.Tuple) []byte {
	if s.fromKey {
		return s.desc.GetField(s.idx, k)
	}
	return s.desc.GetField(s.idx, v)
}

// fieldSources returns, for each column of |cols|, the field of |sch|'s tuples that holds it. It returns false when a
// column is stored with a different encoding than |dest| needs.
func fieldSources(sch schema.Schema, cols []schema.Column, oldKD, oldVD, dest *val.TupleDesc) ([]fieldSource, bool) {
	oldVals := storedCols(sch.GetNonPKCols())
	srcs := make([]fieldSource, len(cols))
	for i, col := range cols {
		var src fieldSource
		if j, ok := sch.GetPKCols().TagToIdx[col.Tag]; ok {
			src = fieldSource{fromKey: true, idx: j, desc: oldKD}
		} else {
			j := -1
			for n, c := range oldVals {
				if c.Tag == col.Tag {
					j = n
				}
			}
			if j < 0 {
				return nil, false
			}
			src = fieldSource{idx: j, desc: oldVD}
		}
		if src.desc.Types[src.idx].Enc != dest.Types[i].Enc {
			return nil, false
		}
		srcs[i] = src
	}
	return srcs, true
}

func storedCols(cc *schema.ColCollection) []schema.Column {
	var cols []schema.Column
	for _, col := range cc.GetColumns() {
		if !col.Virtual {
			cols = append(cols, col)
		}
	}
	return cols
}

func tableHasArtifacts(ctx context.Context, tbl *doltdb.Table) (bool, error) {
	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return false, err
	}
	cnt, err := arts.Count()
	return cnt > 0, err
}
