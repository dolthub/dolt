// Copyright 2024 Dolthub, Inc.
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

package index

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// SecondaryLookupIterGen is responsible for creating secondary
// iterators for lookup joins given a primary key.
type SecondaryLookupIterGen interface {
	New(context.Context, val.Tuple) (prolly.MapIter, error)
	InputKeyDesc() val.TupleDesc
	OutputKeyDesc() val.TupleDesc
	OutputValDesc() val.TupleDesc
	Schema() schema.Schema
	NodeStore() tree.NodeStore
}

type strictLookupIter struct {
	k, v val.Tuple
}

func (i *strictLookupIter) Next(_ context.Context) (k, v val.Tuple, err error) {
	k, v = i.k, i.v
	i.k, i.v = nil, nil
	return k, v, nil
}

type covStrictSecondaryLookupGen struct {
	m          prolly.Map
	prefixDesc val.TupleDesc
	index      *doltIndex
}

var _ SecondaryLookupIterGen = (*covStrictSecondaryLookupGen)(nil)

func (c *covStrictSecondaryLookupGen) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *covStrictSecondaryLookupGen) OutputKeyDesc() val.TupleDesc {
	return c.m.KeyDesc()
}

func (c *covStrictSecondaryLookupGen) OutputValDesc() val.TupleDesc {
	return c.m.ValDesc()
}

func (c *covStrictSecondaryLookupGen) Schema() schema.Schema {
	return c.index.IndexSchema()
}

func (c *covStrictSecondaryLookupGen) NodeStore() tree.NodeStore {
	return c.m.NodeStore()
}

func (c *covStrictSecondaryLookupGen) New(ctx context.Context, k val.Tuple) (prolly.MapIter, error) {
	iter := &strictLookupIter{}
	for i := 0; i < c.prefixDesc.Count(); i++ {
		if k.FieldIsNull(i) {
			// nil field incompatible with strict key lookup
			return iter, nil
		}
	}
	if err := c.m.GetPrefix(ctx, k, c.prefixDesc, func(key val.Tuple, value val.Tuple) error {
		iter.k = key
		iter.v = value
		return nil
	}); err != nil {
		return nil, err
	}
	return iter, nil
}

type nonCovStrictSecondaryLookupGen struct {
	pri        prolly.Map
	sec        prolly.Map
	prefixDesc val.TupleDesc
	sch        schema.Schema
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder
}

func (c *nonCovStrictSecondaryLookupGen) InputKeyDesc() val.TupleDesc {
	return c.sec.KeyDesc()
}

func (c *nonCovStrictSecondaryLookupGen) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *nonCovStrictSecondaryLookupGen) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *nonCovStrictSecondaryLookupGen) Schema() schema.Schema {
	return c.sch
}

func (c *nonCovStrictSecondaryLookupGen) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *nonCovStrictSecondaryLookupGen) New(ctx context.Context, k val.Tuple) (prolly.MapIter, error) {
	var idxKey val.Tuple
	if err := c.sec.GetPrefix(ctx, k, c.prefixDesc, func(key val.Tuple, value val.Tuple) error {
		idxKey = key
		return nil
	}); err != nil {
		return nil, err
	}
	if idxKey == nil {
		return &strictLookupIter{}, nil
	}
	for to := range c.pkMap {
		from := c.pkMap.MapOrdinal(to)
		c.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	pk := c.pkBld.Build(sharePool)

	iter := &strictLookupIter{k: pk}
	if err := c.pri.Get(ctx, pk, func(key val.Tuple, value val.Tuple) error {
		iter.v = value
		return nil
	}); err != nil {
		return nil, err
	}
	return iter, nil
}

type covLaxSecondaryLookupGen struct {
	m          prolly.Map
	index      *doltIndex
	prefixDesc val.TupleDesc
	nullSafe   []bool
}

func (c *covLaxSecondaryLookupGen) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *covLaxSecondaryLookupGen) OutputKeyDesc() val.TupleDesc {
	return c.m.KeyDesc()
}

func (c *covLaxSecondaryLookupGen) OutputValDesc() val.TupleDesc {
	return c.m.ValDesc()
}

func (c *covLaxSecondaryLookupGen) Schema() schema.Schema {
	return c.index.IndexSchema()
}

func (c *covLaxSecondaryLookupGen) NodeStore() tree.NodeStore {
	return c.m.NodeStore()
}

func (c *covLaxSecondaryLookupGen) New(ctx context.Context, k val.Tuple) (prolly.MapIter, error) {
	for i := 0; i < c.prefixDesc.Count(); i++ {
		if k.FieldIsNull(i) && !c.nullSafe[i] {
			return prolly.EmptyPointLookup, nil
		}
	}

	var err error
	if c.prefixDesc.Count() >= c.m.KeyDesc().Count()-1 {
		// key range optimization only works for full length key
		start := k
		stop, ok := prolly.IncrementTuple(ctx, start, c.prefixDesc.Count()-1, c.prefixDesc, c.m.Pool())
		if ok {
			return c.m.IterKeyRange(ctx, start, stop)
		}
	}
	rng := prolly.PrefixRange(ctx, k, c.prefixDesc)

	iter, err := c.m.IterRange(ctx, rng)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

type nonCovLaxSecondaryLookupGen struct {
	pri        prolly.Map
	sec        prolly.Map
	sch        schema.Schema
	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder
	nullSafe   []bool
}

func (c *nonCovLaxSecondaryLookupGen) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *nonCovLaxSecondaryLookupGen) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *nonCovLaxSecondaryLookupGen) Schema() schema.Schema {
	return c.sch
}

func (c *nonCovLaxSecondaryLookupGen) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *nonCovLaxSecondaryLookupGen) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *nonCovLaxSecondaryLookupGen) New(ctx context.Context, k val.Tuple) (prolly.MapIter, error) {
	for i := 0; i < c.prefixDesc.Count(); i++ {
		if k.FieldIsNull(i) && !c.nullSafe[i] {
			return prolly.EmptyPointLookup, nil
		}
	}

	var err error
	if c.prefixDesc.Count() >= c.sec.KeyDesc().Count()-1 {
		// if there are at least cnt-1 fields set in the prefix, the full key
		// is present (the pk columns are appended at the end. at least one pk
		// must not be present in a valid secondary index).
		// TODO: widen this restriction for multiple PKs. need to count the number
		// of PK cols in the index colset vs outside
		start := k
		stop, ok := prolly.IncrementTuple(ctx, start, c.prefixDesc.Count()-1, c.prefixDesc, c.sec.Pool())
		if ok {
			secIter, err := c.sec.IterKeyRange(ctx, start, stop)
			if err != nil {
				return nil, err
			}
			return &nonCoveringMapIter{indexIter: secIter, primary: c.pri, pkMap: c.pkMap, pkBld: c.pkBld}, nil
		}
	}
	rng := prolly.PrefixRange(ctx, k, c.prefixDesc)
	secIter, err := c.sec.IterRange(ctx, rng)
	if err != nil {
		return nil, err
	}

	return &nonCoveringMapIter{indexIter: secIter, primary: c.pri, pkMap: c.pkMap, pkBld: c.pkBld}, nil
}

type keylessSecondaryLookupGen struct {
	pri        prolly.Map
	sec        prolly.Map
	sch        schema.Schema
	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder
}

func (c *keylessSecondaryLookupGen) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *keylessSecondaryLookupGen) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *keylessSecondaryLookupGen) Schema() schema.Schema {
	return c.sch
}

func (c *keylessSecondaryLookupGen) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *keylessSecondaryLookupGen) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *keylessSecondaryLookupGen) New(ctx context.Context, k val.Tuple) (prolly.MapIter, error) {
	var err error
	if c.prefixDesc.Count() == c.sec.KeyDesc().Count() {
		// key range optimization only works if full key
		// keyless indexes should include all rows
		start := k
		stop, ok := prolly.IncrementTuple(ctx, start, c.prefixDesc.Count()-1, c.prefixDesc, c.sec.Pool())
		if ok {
			secIter, err := c.sec.IterKeyRange(ctx, start, stop)
			if err != nil {
				return nil, err
			}
			return &keylessLookupIter{pri: c.pri, secIter: secIter, pkMap: c.pkMap, pkBld: c.pkBld, prefixDesc: c.prefixDesc}, nil
		}
	}
	rng := prolly.PrefixRange(ctx, k, c.prefixDesc)
	secIter, err := c.sec.IterRange(ctx, rng)
	if err != nil {
		return nil, err
	}

	return &keylessLookupIter{pri: c.pri, secIter: secIter, pkMap: c.pkMap, pkBld: c.pkBld, prefixDesc: c.prefixDesc}, nil
}

type keylessLookupIter struct {
	pri     prolly.Map
	secIter prolly.MapIter

	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder

	card uint64
	k, v val.Tuple
}

func (i *keylessLookupIter) Next(ctx context.Context) (k, v val.Tuple, err error) {
	if i.card > 0 {
		// exhaust duplicates
		i.card--
		k, v = i.k, i.v
		return k, v, nil
	}

	// get next secondary key
	idxKey, _, err := i.secIter.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	if idxKey == nil {
		return nil, nil, nil
	}

	// convert sec key to primary key
	for to := range i.pkMap {
		from := i.pkMap.MapOrdinal(to)
		i.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	i.k = i.pkBld.Build(sharePool)

	err = i.pri.Get(ctx, i.k, func(key val.Tuple, value val.Tuple) error {
		i.v = value
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	i.card = val.ReadKeylessCardinality(i.v)

	return i.Next(ctx)
}
