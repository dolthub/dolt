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

// RekeyStatus reports the outcome of [RowRekeyer.RekeyMapStrict].
type RekeyStatus uint8

const (
	// RekeyStatusOK reports a unique, non-NULL new primary key.
	RekeyStatusOK RekeyStatus = iota
	// RekeyStatusNullKey reports a NULL field in the new primary key.
	RekeyStatusNullKey
	// RekeyStatusDuplicateKey reports a duplicate new primary key.
	RekeyStatusDuplicateKey
)

// RowRekeyer rebuilds tuples of one schema under the primary key of another.
// It copies raw field bytes without decoding and is not safe for concurrent
// use.
type RowRekeyer struct {
	ns      tree.NodeStore
	keySrcs []fieldSource
	valSrcs []fieldSource
	kb      *val.TupleBuilder
	vb      *val.TupleBuilder
	oldKD   *val.TupleDesc
	oldVD   *val.TupleDesc
	newKD   *val.TupleDesc
	newVD   *val.TupleDesc
}

// NewRowRekeyer returns a RowRekeyer that rebuilds tuples of |sch| under the
// primary key of |newSch|, which [schema.Rekey] derives from |sch|.
//
// It returns false when either schema is keyless, when |sch| lacks a column of
// |newSch|, or when a column is stored with a different encoding.
func NewRowRekeyer(ns tree.NodeStore, sch, newSch schema.Schema) (*RowRekeyer, bool) {
	// Keyless value tuples start with a hidden cardinality field, so stored
	// column ordinals do not align with tuple fields.
	if schema.IsKeyless(sch) || schema.IsKeyless(newSch) {
		return nil, false
	}
	oldKD, oldVD := sch.GetMapDescriptors(ns)
	newKD, newVD := newSch.GetMapDescriptors(ns)
	oldPK, oldVals := sch.GetPKCols(), sch.GetNonPKCols()
	r := &RowRekeyer{
		ns:    ns,
		kb:    val.NewTupleBuilder(newKD, ns),
		vb:    val.NewTupleBuilder(newVD, ns),
		oldKD: oldKD,
		oldVD: oldVD,
		newKD: newKD,
		newVD: newVD,
	}
	var ok bool
	if r.keySrcs, ok = r.mapFields(oldPK, oldVals, newSch.GetPKCols(), newKD); !ok {
		return nil, false
	}
	if r.valSrcs, ok = r.mapFields(oldPK, oldVals, newSch.GetNonPKCols(), newVD); !ok {
		return nil, false
	}
	return r, true
}

// RekeyMapStrict returns the rows of |rows| keyed by the new primary key.
//
// It returns RekeyStatusOK on success. It stops at the first row with a NULL
// in the new key (RekeyStatusNullKey) or the first duplicate new key
// (RekeyStatusDuplicateKey), returning an empty map.
func (r *RowRekeyer) RekeyMapStrict(ctx context.Context, rows prolly.Map) (prolly.Map, RekeyStatus, error) {
	mut, err := r.newMutableMap(ctx, r.newKD, r.newVD)
	if err != nil {
		return prolly.Map{}, RekeyStatusOK, err
	}
	iter, err := rows.IterAll(ctx)
	if err != nil {
		return prolly.Map{}, RekeyStatusOK, err
	}
	for {
		k, v, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return prolly.Map{}, RekeyStatusOK, err
		}
		nk, status, err := r.validateKey(ctx, mut, k, v)
		if err != nil || status != RekeyStatusOK {
			return prolly.Map{}, status, err
		}
		if err = r.putRow(ctx, mut, nk, k, v); err != nil {
			return prolly.Map{}, RekeyStatusOK, err
		}
	}
	rekeyed, err := mut.Map(ctx)
	return rekeyed, RekeyStatusOK, err
}

// RekeyMap returns the rows of |rows| keyed by the new primary key, along with
// any rejected rows kept under the old primary key.
//
// Rows whose new primary key contains a NULL or duplicates another row's new
// primary key are routed to |rejected|.
func (r *RowRekeyer) RekeyMap(ctx context.Context, rows prolly.Map) (rekeyed, rejected prolly.Map, err error) {
	mut, err := r.newMutableMap(ctx, r.newKD, r.newVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	rejectedMut, err := r.newMutableMap(ctx, r.oldKD, r.oldVD)
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	dupMut, err := r.newMutableMap(ctx, r.newKD, val.NewTupleDescriptor())
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}

	var hasDups bool
	err = IterRows(ctx, rows, func(k, v val.Tuple) error {
		nk, status, err := r.validateKey(ctx, mut, k, v)
		if err != nil {
			return err
		}
		switch status {
		case RekeyStatusNullKey:
			return rejectedMut.Put(ctx, k, v)
		case RekeyStatusDuplicateKey:
			hasDups = true
			if exists, err := dupMut.Has(ctx, nk); err != nil || exists {
				return err
			}
			return dupMut.Put(ctx, nk, val.EmptyTuple)
		}
		return r.putRow(ctx, mut, nk, k, v)
	})
	if err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}

	if hasDups {
		dupKeys, err := dupMut.Map(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, err
		}
		err = IterRows(ctx, rows, func(k, v val.Tuple) error {
			nk, err := r.Key(ctx, k, v)
			if err != nil || r.newKD.HasNulls(nk) {
				return err
			}
			if isDup, err := dupKeys.Has(ctx, nk); err != nil || !isDup {
				return err
			}
			if err = mut.Delete(ctx, nk); err != nil {
				return err
			}
			return rejectedMut.Put(ctx, k, v)
		})
		if err != nil {
			return prolly.Map{}, prolly.Map{}, err
		}
	}

	if rekeyed, err = mut.Map(ctx); err != nil {
		return prolly.Map{}, prolly.Map{}, err
	}
	rejected, err = rejectedMut.Map(ctx)
	return rekeyed, rejected, err
}

// Key builds the new primary key tuple for |k| and |v|.
//
// NULL fields in the new key are preserved. Such a key cannot be stored in a
// map, but callers can still inspect it.
func (r *RowRekeyer) Key(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	return r.buildTuple(ctx, r.kb, r.keySrcs, k, v, true)
}

// validateKey builds the new primary key for |k| and |v| and checks whether it
// contains a NULL or already exists in |mut|.
func (r *RowRekeyer) validateKey(ctx context.Context, mut *prolly.MutableMap, k, v val.Tuple) (val.Tuple, RekeyStatus, error) {
	nk, err := r.Key(ctx, k, v)
	if err != nil {
		return nil, RekeyStatusOK, err
	}
	if r.newKD.HasNulls(nk) {
		return nk, RekeyStatusNullKey, nil
	}
	dup, err := mut.Has(ctx, nk)
	if err != nil {
		return nil, RekeyStatusOK, err
	}
	if dup {
		return nk, RekeyStatusDuplicateKey, nil
	}
	return nk, RekeyStatusOK, nil
}

// putRow builds the destination value tuple for |k| and |v| and inserts it into
// |mut| under |nk|.
func (r *RowRekeyer) putRow(ctx context.Context, mut *prolly.MutableMap, nk, k, v val.Tuple) error {
	nv, err := r.buildTuple(ctx, r.vb, r.valSrcs, k, v, false)
	if err != nil {
		return err
	}
	return mut.Put(ctx, nk, nv)
}

// newMutableMap returns an empty mutable map with descriptors |kd| and |vd|.
func (r *RowRekeyer) newMutableMap(ctx context.Context, kd, vd *val.TupleDesc) (*prolly.MutableMap, error) {
	m, err := prolly.NewMapFromTuples(ctx, r.ns, kd, vd)
	if err != nil {
		return nil, err
	}
	return m.Mutate(), nil
}

// buildTuple populates |b| from |srcs| and builds a tuple.
func (r *RowRekeyer) buildTuple(ctx context.Context, b *val.TupleBuilder, srcs []fieldSource, k, v val.Tuple, permissive bool) (val.Tuple, error) {
	for i, src := range srcs {
		b.PutRaw(i, src.field(k, v))
	}
	if permissive {
		return b.BuildPermissive(ctx, r.ns.Pool())
	}
	return b.Build(ctx, r.ns.Pool())
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
		}
		if err != nil {
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

// mapFields locates each stored column of |target| in |oldPK| or |oldVals| and
// verifies its encoding matches |dest|.
func (r *RowRekeyer) mapFields(oldPK, oldVals, target *schema.ColCollection, dest *val.TupleDesc) ([]fieldSource, bool) {
	srcs := make([]fieldSource, dest.Count())
	for i := range srcs {
		col := target.GetByStoredIndex(i)
		var src fieldSource
		if j, ok := oldPK.TagToIdx[col.Tag]; ok {
			src = fieldSource{desc: r.oldKD, idx: j, fromKey: true}
		} else if j, ok := oldVals.StoredIndexByTag(col.Tag); ok {
			src = fieldSource{desc: r.oldVD, idx: j}
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
