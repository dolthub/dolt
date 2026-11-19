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

var (
	// ErrRekeyNullKey indicates a row has a NULL in its new primary key.
	ErrRekeyNullKey = errors.New("rekeyed row has NULL in primary key")
	// ErrRekeyDuplicateKey indicates two rows share a new primary key.
	ErrRekeyDuplicateKey = errors.New("rekeyed rows share a primary key")
)

// RowRekeyer rebuilds tuples of one schema under the primary key of another.
// It copies their fields, so merges and diffs can compare rows across a
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
// key of |newSch|, which [schema.Rekey] derives from |sch|.
//
// It returns false when either schema is keyless, when |sch| lacks a column of
// |newSch|, or when a column is stored with another encoding.
func NewRowRekeyer(ns tree.NodeStore, sch, newSch schema.Schema) (*RowRekeyer, bool) {
	if schema.IsKeyless(sch) || schema.IsKeyless(newSch) {
		return nil, false
	}
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	oldPK, oldVals := sch.GetPKCols(), sch.GetNonPKCols()
	keySrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, newSch.GetPKCols().GetColumns(), newKD)
	if !ok {
		return nil, false
	}
	valSrc, ok := fieldSources(oldPK, oldVals, oldKD, oldVD, newSch.GetNonPKCols().StoredColumns(), newVD)
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
	nv, err := r.value(ctx, k, v)
	if err != nil {
		return nil, nil, err
	}
	return nk, nv, nil
}

// Key returns the new primary key of |k| and |v|, without the new value.
//
// A NULL in the new key stays NULL. A map cannot key a row by such a NULL, but
// the row can still be read.
func (r *RowRekeyer) Key(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	return r.buildTuple(ctx, r.kb, r.keySrc, k, v, true)
}

// RekeyMap returns the rows of |rows| keyed by the new primary key, and the
// unpaired rows.
//
// A primary key holds one row and no NULL. Rows with a NULL in the new key are
// unpaired, and so is every row whose new key is duplicated. hasNullKey reports
// whether any row has a NULL in the new key.
func (r *RowRekeyer) RekeyMap(ctx context.Context, rows prolly.Map) (rekeyed, unpairedRows prolly.Map, hasNullKey bool, err error) {
	mut, err := r.newTargetMutator(ctx)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	emptyOld, err := prolly.NewMapFromTuples(ctx, r.ns, r.oldKD, r.oldVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	unpaired := emptyOld.Mutate()

	var dups *prolly.MutableMap
	err = IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, err := r.Key(ctx, k, v)
		if err != nil {
			return err
		}
		if r.newKD.HasNulls(nk) {
			hasNullKey = true
			return unpaired.Put(ctx, k, v)
		}
		if !r.isUnique {
			dup, err := mut.Has(ctx, nk)
			if err != nil {
				return err
			}
			if dup {
				return r.recordDuplicate(ctx, &dups, nk)
			}
		}
		return r.putRow(ctx, mut, nk, k, v)
	})
	if err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	if dups != nil {
		if err = r.moveDuplicateKeys(ctx, rows, mut, dups, unpaired); err != nil {
			return prolly.Map{}, prolly.Map{}, false, err
		}
	}
	if rekeyed, err = mut.Map(ctx); err != nil {
		return prolly.Map{}, prolly.Map{}, false, err
	}
	unpairedRows, err = unpaired.Map(ctx)
	return rekeyed, unpairedRows, hasNullKey, err
}

// RekeyUniqueMap returns the rows of |rows| keyed by the new primary key.
//
// It stops at the first row with a NULL in the new key (ErrRekeyNullKey) or
// the first duplicate new key (ErrRekeyDuplicateKey).
func (r *RowRekeyer) RekeyUniqueMap(ctx context.Context, rows prolly.Map) (prolly.Map, error) {
	mut, err := r.newTargetMutator(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	err = IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, err := r.Key(ctx, k, v)
		if err != nil {
			return err
		}
		if r.newKD.HasNulls(nk) {
			return ErrRekeyNullKey
		}
		if !r.isUnique {
			dup, err := mut.Has(ctx, nk)
			if err != nil {
				return err
			}
			if dup {
				return ErrRekeyDuplicateKey
			}
		}
		return r.putRow(ctx, mut, nk, k, v)
	})
	if err != nil {
		return prolly.Map{}, err
	}
	return mut.Map(ctx)
}

// newTargetMutator returns an empty mutable map for the destination schema.
func (r *RowRekeyer) newTargetMutator(ctx context.Context) (*prolly.MutableMap, error) {
	emptyNew, err := prolly.NewMapFromTuples(ctx, r.ns, r.newKD, r.newVD)
	if err != nil {
		return nil, err
	}
	return emptyNew.Mutate(), nil
}

// putRow builds the destination value tuple for |k| and |v| and inserts it into
// |mut| under |nk|.
func (r *RowRekeyer) putRow(ctx context.Context, mut *prolly.MutableMap, nk, k, v val.Tuple) error {
	nv, err := r.value(ctx, k, v)
	if err != nil {
		return err
	}
	return mut.Put(ctx, nk, nv)
}

// recordDuplicate adds |nk| to the key-only duplicate set |dups|, creating it
// on first collision and skipping keys already recorded.
func (r *RowRekeyer) recordDuplicate(ctx context.Context, dups **prolly.MutableMap, nk val.Tuple) error {
	if *dups == nil {
		emptyDups, err := prolly.NewMapFromTuples(ctx, r.ns, r.newKD, val.NewTupleDescriptor())
		if err != nil {
			return err
		}
		*dups = emptyDups.Mutate()
		return (*dups).Put(ctx, nk, val.EmptyTuple)
	}
	exists, err := (*dups).Has(ctx, nk)
	if err != nil || exists {
		return err
	}
	return (*dups).Put(ctx, nk, val.EmptyTuple)
}

// moveDuplicateKeys moves every row whose new key is in |dups| from |mut| to
// |unpaired|.
//
// It rekeys |rows| again, since |mut| keeps the first row of each duplicate key
// and |dups| holds the colliding keys.
func (r *RowRekeyer) moveDuplicateKeys(ctx context.Context, rows prolly.Map, mut, dups, unpaired *prolly.MutableMap) error {
	dupKeys, err := dups.Map(ctx)
	if err != nil {
		return err
	}
	return IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, err := r.Key(ctx, k, v)
		if err != nil {
			return err
		}
		if r.newKD.HasNulls(nk) {
			return nil
		}
		isDup, err := dupKeys.Has(ctx, nk)
		if err != nil || !isDup {
			return err
		}
		if err = mut.Delete(ctx, nk); err != nil {
			return err
		}
		return unpaired.Put(ctx, k, v)
	})
}

// value returns the new value tuple of |k| and |v|.
func (r *RowRekeyer) value(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	return r.buildTuple(ctx, r.vb, r.valSrc, k, v, false)
}

// buildTuple populates |b| from |srcs| and builds a tuple, recycling |b| on
// error so subsequent rows do not observe stale fields.
func (r *RowRekeyer) buildTuple(ctx context.Context, b *val.TupleBuilder, srcs []fieldSource, k, v val.Tuple, permissive bool) (val.Tuple, error) {
	for i, src := range srcs {
		b.PutRaw(i, src.field(k, v))
	}
	var tup val.Tuple
	var err error
	if permissive {
		tup, err = b.BuildPermissive(ctx, r.ns.Pool())
	} else {
		tup, err = b.Build(ctx, r.ns.Pool())
	}
	if err != nil {
		b.Recycle()
		return nil, err
	}
	return tup, nil
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
}

// field returns the raw field that |s| locates in |k| or |v|.
func (s fieldSource) field(k, v val.Tuple) []byte {
	if s.fromKey {
		return s.desc.GetField(s.idx, k)
	}
	return s.desc.GetField(s.idx, v)
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
		if j, ok := oldPK.TagToIdx[col.Tag]; ok {
			src = fieldSource{desc: oldKD, idx: j, fromKey: true}
		} else if j, ok := oldVals.StoredIndexByTag(col.Tag); ok {
			src = fieldSource{desc: oldVD, idx: j}
		} else {
			return nil, false
		}
		if src.desc.Types[src.idx].Enc != dest.Types[i].Enc {
			return nil, false
		}
		srcs[i] = src
	}
	return srcs, true
}
