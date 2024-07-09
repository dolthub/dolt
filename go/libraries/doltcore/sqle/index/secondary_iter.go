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
	"io"
)

type SecondaryLookupIter interface {
	New(context.Context, val.Tuple) error
	Next(context.Context) (k, v val.Tuple, ok bool, err error)
	InputKeyDesc() val.TupleDesc
	OutputKeyDesc() val.TupleDesc
	OutputValDesc() val.TupleDesc
	Schema() schema.Schema
	NodeStore() tree.NodeStore
}

type coveringStrictSecondaryLookup struct {
	m          prolly.Map
	k, v       val.Tuple
	prefixDesc val.TupleDesc
	index      *doltIndex
}

var _ SecondaryLookupIter = (*coveringStrictSecondaryLookup)(nil)

func (c *coveringStrictSecondaryLookup) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *coveringStrictSecondaryLookup) OutputKeyDesc() val.TupleDesc {
	return c.m.KeyDesc()
}

func (c *coveringStrictSecondaryLookup) OutputValDesc() val.TupleDesc {
	return c.m.ValDesc()
}

func (c *coveringStrictSecondaryLookup) Schema() schema.Schema {
	return c.index.IndexSchema()
}

func (c *coveringStrictSecondaryLookup) NodeStore() tree.NodeStore {
	return c.m.NodeStore()
}

func (c *coveringStrictSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	if k.FieldIsNull(0) {
		c.k, c.v = nil, nil
		return nil
	}
	return c.m.GetPrefix(ctx, k, c.prefixDesc, func(key val.Tuple, value val.Tuple) error {
		c.k = key
		c.v = value
		return nil
	})
}

func (c *coveringStrictSecondaryLookup) Next(_ context.Context) (k, v val.Tuple, ok bool, err error) {
	k, v = c.k, c.v
	c.k, c.v = nil, nil
	ok = k != nil
	return
}

type nonCoveringStrictSecondaryLookup struct {
	pri   prolly.Map
	sec   prolly.Map
	sch   schema.Schema
	k, v  val.Tuple
	pkMap val.OrdinalMapping
	pkBld *val.TupleBuilder
}

func (c *nonCoveringStrictSecondaryLookup) InputKeyDesc() val.TupleDesc {
	return c.sec.KeyDesc()
}

func (c *nonCoveringStrictSecondaryLookup) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *nonCoveringStrictSecondaryLookup) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *nonCoveringStrictSecondaryLookup) Schema() schema.Schema {
	return c.sch
}

func (c *nonCoveringStrictSecondaryLookup) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *nonCoveringStrictSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	var idxKey val.Tuple
	err := c.sec.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		idxKey = key
		return nil
	})
	for to := range c.pkMap {
		from := c.pkMap.MapOrdinal(to)
		c.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	c.k = c.pkBld.Build(sharePool)

	if err != nil {
		return err
	}
	return c.pri.Get(ctx, c.k, func(key val.Tuple, value val.Tuple) error {
		c.v = value
		return nil
	})
}

func (c *nonCoveringStrictSecondaryLookup) Next(_ context.Context) (k, v val.Tuple, ok bool, err error) {
	k, v = c.k, c.v
	c.k, c.v = nil, nil
	ok = k != nil
	return k, v, ok, nil
}

type coveringLaxSecondaryLookup struct {
	m          prolly.Map
	index      *doltIndex
	prefixDesc val.TupleDesc
	k, v       val.Tuple
	iter       prolly.MapIter
}

func (c *coveringLaxSecondaryLookup) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *coveringLaxSecondaryLookup) OutputKeyDesc() val.TupleDesc {
	return c.m.KeyDesc()
}

func (c *coveringLaxSecondaryLookup) OutputValDesc() val.TupleDesc {
	return c.m.ValDesc()
}

func (c *coveringLaxSecondaryLookup) Schema() schema.Schema {
	return c.index.Schema()
}

func (c *coveringLaxSecondaryLookup) NodeStore() tree.NodeStore {
	return c.m.NodeStore()
}

func (c *coveringLaxSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	// if no nil fields can do lookup
	//var lax bool
	//for i := 0; i < k.Count(); i++ {
	//	if k.FieldIsNull(i) {
	//		lax = true
	//		break
	//	}
	//}
	//
	//if !lax {
	//	return c.m.GetPrefix(ctx, k, c.prefixDesc, func(key val.Tuple, value val.Tuple) error {
	//		c.k = key
	//		c.v = value
	//		return nil
	//	})
	//}

	start := k
	stop, ok := prolly.IncrementTuple(start, c.prefixDesc.Count()-1, c.prefixDesc, c.m.Pool())
	var err error
	if ok {
		c.iter, err = c.m.IterKeyRange(ctx, start, stop)
		if err != nil {
			return err
		}
	} else {
		// TODO inline a prolly range
		var rng prolly.Range
		c.iter, err = c.m.IterRange(ctx, rng)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *coveringLaxSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, ok bool, err error) {
	if c.iter == nil {
		k, v = c.k, c.v
		c.k, c.v = nil, nil
		ok = k != nil
		return k, v, ok, nil
	}
	k, v, err = c.iter.Next(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, nil, false, err
		}
		return nil, nil, false, nil
	}
	return k, v, true, nil
}

type nonCoveringLaxSecondaryLookup struct {
	pri        prolly.Map
	sec        prolly.Map
	sch        schema.Schema
	k, v       val.Tuple
	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder
	secIter    prolly.MapIter
}

func (c *nonCoveringLaxSecondaryLookup) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *nonCoveringLaxSecondaryLookup) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *nonCoveringLaxSecondaryLookup) Schema() schema.Schema {
	return c.sch
}

func (c *nonCoveringLaxSecondaryLookup) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *nonCoveringLaxSecondaryLookup) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *nonCoveringLaxSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	var lax bool
	for i := 0; i < k.Count(); i++ {
		if k.FieldIsNull(i) {
			lax = true
			break
		}
	}
	if !lax {
		var idxKey val.Tuple
		err := c.sec.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
			idxKey = key
			return nil
		})
		for to := range c.pkMap {
			from := c.pkMap.MapOrdinal(to)
			c.pkBld.PutRaw(to, idxKey.GetField(from))
		}
		c.k = c.pkBld.Build(sharePool)

		if err != nil {
			return err
		}
		return c.pri.Get(ctx, c.k, func(key val.Tuple, value val.Tuple) error {
			c.v = value
			return nil
		})
	}

	start := k
	stop, ok := prolly.IncrementTuple(start, c.prefixDesc.Count()-1, c.prefixDesc, c.sec.Pool())
	var err error
	if ok {
		c.secIter, err = c.sec.IterKeyRange(ctx, start, stop)
		if err != nil {
			return err
		}
	} else {
		// TODO inline a prolly range
		var rng prolly.Range
		c.secIter, err = c.sec.IterRange(ctx, rng)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *nonCoveringLaxSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, ok bool, err error) {
	if c.secIter == nil {
		// strict key already found
		k, v = c.k, c.v
		c.k, c.v = nil, nil
		ok = k != nil
		return k, v, ok, nil
	}
	// get secondary key
	idxKey, _, err := c.secIter.Next(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, nil, false, err
		}
		c.secIter = nil
	}

	// convert sec key to primary key
	for to := range c.pkMap {
		from := c.pkMap.MapOrdinal(to)
		c.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	k = c.pkBld.Build(sharePool)

	if err != nil {
		return nil, nil, false, err
	}

	// primary key lookup
	err = c.pri.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		v = value
		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}

	return k, v, c.secIter != nil, nil
}

type keylessSecondaryLookup struct {
	pri        prolly.Map
	sec        prolly.Map
	sch        schema.Schema
	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder

	secIter prolly.MapIter
	k, v    val.Tuple
	card    uint64
}

func (c *keylessSecondaryLookup) InputKeyDesc() val.TupleDesc {
	return c.prefixDesc
}

func (c *keylessSecondaryLookup) OutputKeyDesc() val.TupleDesc {
	return c.pri.KeyDesc()
}

func (c *keylessSecondaryLookup) Schema() schema.Schema {
	return c.sch
}

func (c *keylessSecondaryLookup) OutputValDesc() val.TupleDesc {
	return c.pri.ValDesc()
}

func (c *keylessSecondaryLookup) NodeStore() tree.NodeStore {
	return c.pri.NodeStore()
}

func (c *keylessSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	// check lax or strict
	// source index could be strict or lax
	var lax bool
	for i := 0; i < k.Count(); i++ {
		if k.FieldIsNull(i) {
			lax = true
			break
		}
	}
	if !lax { // TODO also check uniqueness
		var idxKey val.Tuple
		err := c.sec.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
			idxKey = key
			return nil
		})
		for to := range c.pkMap {
			from := c.pkMap.MapOrdinal(to)
			c.pkBld.PutRaw(to, idxKey.GetField(from))
		}
		c.k = c.pkBld.Build(sharePool)

		err = c.pri.Get(ctx, c.k, func(key val.Tuple, value val.Tuple) error {
			c.v = value
			return nil
		})
		if err != nil {
			return err
		}

		c.card = val.ReadKeylessCardinality(c.v)
		return nil
	}

	start := k
	stop, ok := prolly.IncrementTuple(start, c.prefixDesc.Count()-1, c.prefixDesc, c.sec.Pool())
	var err error
	if ok {
		c.secIter, err = c.sec.IterKeyRange(ctx, start, stop)
		if err != nil {
			return err
		}
	} else {
		// TODO inline a prolly range
		var rng prolly.Range
		c.secIter, err = c.sec.IterRange(ctx, rng)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *keylessSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, ok bool, err error) {
	if c.card > 0 {
		// exhaust duplicates
		c.card--
		k, v = c.k, c.v
		return k, v, true, nil
	}

	if c.secIter == nil {
		// original lookup was strict, done
		return nil, nil, false, err
	}

	// get next secondary key
	idxKey, _, err := c.secIter.Next(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, nil, false, err
		}
		c.secIter = nil
	}

	// convert sec key to primary key
	for to := range c.pkMap {
		from := c.pkMap.MapOrdinal(to)
		c.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	c.k = c.pkBld.Build(sharePool)

	err = c.pri.Get(ctx, c.k, func(key val.Tuple, value val.Tuple) error {
		c.v = value
		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}

	c.card = val.ReadKeylessCardinality(c.v)

	return c.Next(ctx)

}
