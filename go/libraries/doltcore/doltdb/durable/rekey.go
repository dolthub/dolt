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
	"slices"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// RowRekeyer rebuilds tuples stored with one schema under the primary key of
// another, by copying their fields. It is not safe for concurrent use.
//
// Merges and diffs use it to compare a table across a primary key change.
type RowRekeyer struct {
	ns     tree.NodeStore
	keySrc []fieldSource
	valSrc []fieldSource
	kb     *val.TupleBuilder
	vb     *val.TupleBuilder
	oldKD  *val.TupleDesc
	oldVD  *val.TupleDesc
	newKD  *val.TupleDesc
	newVD  *val.TupleDesc
	// isUnique reports whether the new key contains the whole old key, which
	// makes every new key unique without checking.
	isUnique bool
}

// NewRowRekeyer returns a RowRekeyer from |sch| to |newSch|, a schema that
// [schema.RekeyedSchema] built from |sch|, writing to |ns|. It returns false
// when |sch| lacks a column of |newSch| or stores it with another encoding.
func NewRowRekeyer(ns tree.NodeStore, sch, newSch schema.Schema) (*RowRekeyer, bool) {
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	oldPK, oldVals := sch.GetPKCols(), storedCols(sch.GetNonPKCols())
	keySrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, newSch.GetPKCols().GetColumns(), newKD)
	if !ok {
		return nil, false
	}
	valSrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, storedCols(newSch.GetNonPKCols()), newVD)
	if !ok {
		return nil, false
	}
	isUnique := true
	for _, tag := range sch.GetPKCols().Tags {
		if _, ok := newSch.GetPKCols().TagToIdx[tag]; !ok {
			isUnique = false
		}
	}
	return &RowRekeyer{
		ns:       ns,
		keySrc:   keySrc,
		valSrc:   valSrc,
		kb:       val.NewTupleBuilder(newKD, ns),
		vb:       val.NewTupleBuilder(newVD, ns),
		oldKD:    oldKD,
		oldVD:    oldVD,
		newKD:    newKD,
		newVD:    newVD,
		isUnique: isUnique,
	}, true
}

// Rekey returns the tuple pair |k|, |v| rebuilt under the new primary key.
//
// The new key holds a NULL where HasNullKey reports one. Such a key fits no
// map, so RekeyMap keeps its row apart, but a diff can still show it.
func (r *RowRekeyer) Rekey(ctx context.Context, k, v val.Tuple) (val.Tuple, val.Tuple, error) {
	for i, src := range r.keySrc {
		r.kb.PutRaw(i, src.field(k, v))
	}
	for i, src := range r.valSrc {
		r.vb.PutRaw(i, src.field(k, v))
	}
	nk, err := r.kb.BuildPermissive(ctx, r.ns.Pool())
	if err != nil {
		return nil, nil, err
	}
	nv, err := r.vb.Build(ctx, r.ns.Pool())
	if err != nil {
		return nil, nil, err
	}
	return nk, nv, nil
}

// HasNullKey reports whether the new primary key of |k|, |v| would hold a
// NULL, which no primary key can.
func (r *RowRekeyer) HasNullKey(k, v val.Tuple) bool {
	return slices.ContainsFunc(r.keySrc, func(src fieldSource) bool {
		return src.field(k, v) == nil
	})
}

// RekeyMap returns |rows| keyed by the new primary key, and apart, as stored
// in |rows|, the rows it cannot key.
//
// A key holds one row and no NULL, so rows that share a new key with another
// or would hold a NULL in it have no place in the first map. Callers decide
// what they mean: a diff shows them unpaired, and a merge refuses them.
func (r *RowRekeyer) RekeyMap(ctx context.Context, rows prolly.Map) (prolly.Map, prolly.Map, error) {
	emptyNew, err := prolly.NewMapFromTuples(ctx, r.ns, r.newKD, r.newVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	emptyOld, err := prolly.NewMapFromTuples(ctx, r.ns, r.oldKD, r.oldVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}

	mut, unpaired := emptyNew.Mutate(), emptyOld.Mutate()
	var shared *prolly.MutableMap
	err = IterRows(ctx, rows, func(k, v val.Tuple) error {
		if r.HasNullKey(k, v) {
			return unpaired.Put(ctx, k, v)
		}
		nk, nv, err := r.Rekey(ctx, k, v)
		if err != nil {
			return err
		}
		if !r.isUnique {
			isShared, err := mut.Has(ctx, nk)
			if err != nil {
				return err
			}
			if isShared {
				if shared == nil {
					shared = emptyNew.Mutate()
				}
				return shared.Put(ctx, nk, nv)
			}
		}
		return mut.Put(ctx, nk, nv)
	})
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	if shared != nil {
		if err = r.splitSharedKeys(ctx, rows, mut, shared, unpaired); err != nil {
			return prolly.Map{}, prolly.Map{}, err
		}
	}
	rekeyed, err := mut.Map(ctx)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	unpairedRows, err := unpaired.Map(ctx)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	return rekeyed, unpairedRows, nil
}

// splitSharedKeys moves the rows whose new key is in |shared| out of |mut| and
// into |unpaired|.
//
// It rekeys |rows| a second time because |mut| holds the first row of each
// such key and |shared| only the later ones.
func (r *RowRekeyer) splitSharedKeys(ctx context.Context, rows prolly.Map, mut, shared, unpaired *prolly.MutableMap) error {
	sharedKeys, err := shared.Map(ctx)
	if err != nil {
		return err
	}
	return IterRows(ctx, rows, func(k, v val.Tuple) error {
		if r.HasNullKey(k, v) {
			return nil
		}
		nk, _, err := r.Rekey(ctx, k, v)
		if err != nil {
			return err
		}
		isShared, err := sharedKeys.Has(ctx, nk)
		if err != nil {
			return err
		}
		if !isShared {
			return nil
		}
		if err = mut.Delete(ctx, nk); err != nil {
			return err
		}
		return unpaired.Put(ctx, k, v)
	})
}

// IterRows calls |cb| with each key and value of |rows|, stopping at the first
// error.
func IterRows(ctx context.Context, rows prolly.Map, cb func(k, v val.Tuple) error) error {
	iter, err := rows.IterAll(ctx)
	if err != nil {
		return err
	}
	for {
		k, v, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if err = cb(k, v); err != nil {
			return err
		}
	}
}

// fieldSource locates one field of a rekeyed tuple in the old key or value.
type fieldSource struct {
	desc    *val.TupleDesc
	idx     int
	fromKey bool
}

// field returns the field that s locates in |k| or |v|.
func (s fieldSource) field(k, v val.Tuple) []byte {
	if s.fromKey {
		return s.desc.GetField(s.idx, k)
	}
	return s.desc.GetField(s.idx, v)
}

// fieldSources locates each column of |cols| in the old key or value. It
// returns false when a column is missing or its encoding differs from the one
// |dest| needs.
//
// The old key stores |oldPK| as |oldKD| describes, and the old value stores
// |oldVals| as |oldVD| describes.
func fieldSources(oldPK *schema.ColCollection, oldVals []schema.Column, oldKD, oldVD *val.TupleDesc, cols []schema.Column, dest *val.TupleDesc) ([]fieldSource, bool) {
	srcs := make([]fieldSource, len(cols))
	for i, col := range cols {
		var src fieldSource
		if j, ok := oldPK.TagToIdx[col.Tag]; ok {
			src = fieldSource{desc: oldKD, idx: j, fromKey: true}
		} else {
			j := slices.IndexFunc(oldVals, func(c schema.Column) bool { return c.Tag == col.Tag })
			if j < 0 {
				return nil, false
			}
			src = fieldSource{desc: oldVD, idx: j}
		}
		if src.desc.Types[src.idx].Enc != dest.Types[i].Enc {
			return nil, false
		}
		srcs[i] = src
	}
	return srcs, true
}

// storedCols returns the columns of |cc| that tuples store. Virtual columns
// are computed on read, so tuples do not hold them.
func storedCols(cc *schema.ColCollection) []schema.Column {
	var cols []schema.Column
	for _, col := range cc.GetColumns() {
		if !col.Virtual {
			cols = append(cols, col)
		}
	}
	return cols
}
