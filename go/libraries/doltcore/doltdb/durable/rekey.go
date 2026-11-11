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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// RowRekeyer rebuilds tuples of one schema under the primary key of another,
// by copying their fields, so merges and diffs can compare rows across a
// primary key change. It is not safe for concurrent use.
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

// NewRowRekeyer returns a RowRekeyer that rebuilds tuples of |sch| under the
// key of |newSch|, which [schema.Rekeyed] derives from |sch|.
//
// It returns false when |sch| lacks a column of |newSch| or stores it with
// another encoding.
func NewRowRekeyer(ns tree.NodeStore, sch, newSch schema.Schema) (*RowRekeyer, bool) {
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	oldPK, oldVals := sch.GetPKCols(), sch.GetNonPKCols()
	keySrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, newSch.GetPKCols().GetColumns(), newKD)
	if !ok {
		return nil, false
	}
	valSrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, newSch.GetNonPKCols().GetStoredColumns(), newVD)
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

// Rekey returns |k| and |v| rebuilt under the new primary key.
func (r *RowRekeyer) Rekey(ctx context.Context, k, v val.Tuple) (val.Tuple, val.Tuple, error) {
	nk, err := r.Key(ctx, k, v)
	if err != nil {
		return nil, nil, err
	}
	for i, src := range r.valSrc {
		r.vb.PutRaw(i, src.field(k, v))
	}
	nv, err := r.vb.Build(ctx, r.ns.Pool())
	if err != nil {
		return nil, nil, err
	}
	return nk, nv, nil
}

// Key returns the new primary key of |k| and |v|, without the new value.
//
// A NULL in the new key stays NULL, and so does an ENUM or SET with a member
// the new key lacks. A map cannot key a row by such a NULL, but the row can
// still be read.
func (r *RowRekeyer) Key(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	for i, src := range r.keySrc {
		if src.members == nil {
			r.kb.PutRaw(i, src.field(k, v))
			continue
		}
		stored, ok, err := src.remapped(ctx, r.ns, k, v)
		if err != nil {
			return nil, err
		}
		if !ok {
			r.kb.PutRaw(i, nil)
			continue
		}
		if err = tree.PutField(ctx, r.ns, r.kb, i, stored); err != nil {
			return nil, err
		}
	}
	return r.kb.BuildPermissive(ctx, r.ns.Pool())
}

// hasNullField reports whether the rekeyed key |nk| holds a NULL, which no
// primary key can.
func (r *RowRekeyer) hasNullField(nk val.Tuple) bool {
	for i := 0; i < r.newKD.Count(); i++ {
		if nk.FieldIsNull(i) {
			return true
		}
	}
	return false
}

// RekeyMap returns the rows of |rows| keyed by the new primary key, and the
// unpaired rows.
//
// A primary key holds one row and no NULL. Rows with a NULL in the new key are
// unpaired, and so is every row whose new key is shared. hasNullKey reports
// whether any row has a NULL in the new key.
func (r *RowRekeyer) RekeyMap(ctx context.Context, rows prolly.Map) (rekeyed, unpairedRows prolly.Map, hasNullKey bool, err error) {
	emptyNew, err := prolly.NewMapFromTuples(ctx, r.ns, r.newKD, r.newVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	emptyOld, err := prolly.NewMapFromTuples(ctx, r.ns, r.oldKD, r.oldVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}

	mut, unpaired := emptyNew.Mutate(), emptyOld.Mutate()
	var shared *prolly.MutableMap
	err = IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, nv, err := r.Rekey(ctx, k, v)
		if err != nil {
			return err
		}
		if r.hasNullField(nk) {
			hasNullKey = true
			return unpaired.Put(ctx, k, v)
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
		return prolly.Map{}, prolly.Map{}, false, err
	}
	if shared != nil {
		if err = r.splitSharedKeys(ctx, rows, mut, shared, unpaired); err != nil {
			return prolly.Map{}, prolly.Map{}, false, err
		}
	}
	if rekeyed, err = mut.Map(ctx); err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	unpairedRows, err = unpaired.Map(ctx)
	return rekeyed, unpairedRows, hasNullKey, err
}

// splitSharedKeys moves every row whose new key is in |shared| from |mut| to
// |unpaired|.
//
// It rekeys |rows| again, since |mut| keeps the first row of each shared key
// and |shared| holds the rest.
func (r *RowRekeyer) splitSharedKeys(ctx context.Context, rows prolly.Map, mut, shared, unpaired *prolly.MutableMap) error {
	sharedKeys, err := shared.Map(ctx)
	if err != nil {
		return err
	}
	return IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, err := r.Key(ctx, k, v)
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

// IterRows calls |cb| for each key and value of |rows|. It returns the first
// error from |cb| or from the iterator.
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
	// members remaps the old field to the new field when its ENUM or SET
	// members moved. It is nil otherwise.
	members sql.MemberMap
}

// field returns the raw field that |s| locates in |k| or |v|.
func (s fieldSource) field(k, v val.Tuple) []byte {
	if s.fromKey {
		return s.desc.GetField(s.idx, k)
	}
	return s.desc.GetField(s.idx, v)
}

// remapped returns the ENUM or SET field that |s| locates, with its members
// stored as in the new type.
//
// It returns false when the field is NULL or holds a member the new type
// lacks.
func (s fieldSource) remapped(ctx context.Context, ns tree.NodeStore, k, v val.Tuple) (interface{}, bool, error) {
	tup := v
	if s.fromKey {
		tup = k
	}
	stored, err := tree.GetField(ctx, s.desc, s.idx, tup, ns)
	if err != nil || stored == nil {
		return nil, false, err
	}
	remapped, ok := s.members.Remap(stored)
	return remapped, ok, nil
}

// fieldSources returns the field of the old key or value that holds each
// column of |cols|, read with |oldKD| or |oldVD|.
//
// It returns false when a column is in neither |oldPK| nor |oldVals|, or when
// its encoding differs from |dest|.
func fieldSources(oldPK, oldVals *schema.ColCollection, oldKD, oldVD *val.TupleDesc, cols []schema.Column, dest *val.TupleDesc) ([]fieldSource, bool) {
	srcs := make([]fieldSource, len(cols))
	for i, col := range cols {
		var src fieldSource
		var oldCol schema.Column
		if j, ok := oldPK.TagToIdx[col.Tag]; ok {
			src = fieldSource{desc: oldKD, idx: j, fromKey: true}
			oldCol = oldPK.GetByIndex(j)
		} else if j, ok := oldVals.StoredIndexByTag(col.Tag); ok {
			src = fieldSource{desc: oldVD, idx: j}
			oldCol = oldVals.GetByStoredIndex(j)
		} else {
			return nil, false
		}
		if src.desc.Types[src.idx].Enc != dest.Types[i].Enc {
			return nil, false
		}
		if members, ok := oldCol.MapMembers(col); ok && !members.IsIdentity() {
			src.members = members
		}
		srcs[i] = src
	}
	return srcs, true
}
