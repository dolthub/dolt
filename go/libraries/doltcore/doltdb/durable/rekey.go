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

package durable

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrRekeyCollision = errors.New("two rows have the same new primary key")
var ErrRekeyNullKey = errors.New("a row has a NULL in the new primary key")

// RekeyRows returns |rows|, stored under |sch|, rewritten under |newSch|, a schema with the same columns and a
// different primary key, such as one made by [schema.RekeyedSchema]. It returns false when a column of |newSch| is
// missing from |sch| or stored with another encoding. It returns ErrRekeyCollision or ErrRekeyNullKey when the rows
// cannot be keyed by the new primary key. The new map is written to |ns|.
func RekeyRows(ctx context.Context, ns tree.NodeStore, rows prolly.Map, sch, newSch schema.Schema) (prolly.Map, bool, error) {
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	keySrc, ok := fieldSources(sch, newSch.GetPKCols().GetColumns(), oldKD, oldVD, newKD)
	if !ok {
		return prolly.Map{}, false, nil
	}
	valSrc, ok := fieldSources(sch, storedCols(newSch.GetNonPKCols()), oldKD, oldVD, newVD)
	if !ok {
		return prolly.Map{}, false, nil
	}

	emptyRows, err := prolly.NewMapFromTuples(ctx, ns, newKD, newVD)
	if err != nil {
		return prolly.Map{}, false, err
	}
	mut := emptyRows.Mutate()

	pool := rows.Pool()
	kb, vb := val.NewTupleBuilder(newKD, ns), val.NewTupleBuilder(newVD, ns)
	iter, err := rows.IterAll(ctx)
	if err != nil {
		return prolly.Map{}, false, err
	}
	for {
		k, v, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return prolly.Map{}, false, err
		}
		for i, src := range keySrc {
			field := src.get(k, v)
			if field == nil {
				return prolly.Map{}, false, ErrRekeyNullKey
			}
			kb.PutRaw(i, field)
		}
		for i, src := range valSrc {
			vb.PutRaw(i, src.get(k, v))
		}
		nk, err := kb.Build(ctx, pool)
		if err != nil {
			return prolly.Map{}, false, err
		}
		nv, err := vb.Build(ctx, pool)
		if err != nil {
			return prolly.Map{}, false, err
		}
		if dup, err := mut.Has(ctx, nk); err != nil {
			return prolly.Map{}, false, err
		} else if dup {
			return prolly.Map{}, false, ErrRekeyCollision
		}
		if err = mut.Put(ctx, nk, nv); err != nil {
			return prolly.Map{}, false, err
		}
	}
	newRows, err := mut.Map(ctx)
	if err != nil {
		return prolly.Map{}, false, err
	}
	return newRows, true, nil
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
// column is missing from |sch| or stored with a different encoding than |dest| needs.
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
