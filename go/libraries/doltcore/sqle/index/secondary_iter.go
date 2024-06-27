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
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
	"io"
)

type SecondaryLookupIter interface {
	New(context.Context, val.Tuple) error
	Next(context.Context) (k, v val.Tuple, more bool, err error)
}

type coveringStrictSecondaryLookup struct {
	m    prolly.Map
	k, v val.Tuple
}

var _ SecondaryLookupIter = (*coveringStrictSecondaryLookup)(nil)

func (c *coveringStrictSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	return c.m.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
		c.k = key
		c.v = value
		return nil
	})
}

func (c *coveringStrictSecondaryLookup) Next(_ context.Context) (k, v val.Tuple, more bool, err error) {
	k, v, more = c.k, c.v, false
	c.k, c.v = nil, nil
	return
}

type nonCoveringStrictSecondaryLookup struct {
	pri   prolly.Map
	sec   prolly.Map
	k, v  val.Tuple
	pkMap val.OrdinalMapping
	pkBld *val.TupleBuilder
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

func (c *nonCoveringStrictSecondaryLookup) Next(_ context.Context) (k, v val.Tuple, more bool, err error) {
	k, v, more = c.k, c.v, false
	c.k, c.v = nil, nil
	return
}

type coveringLaxSecondaryLookup struct {
	m          prolly.Map
	prefixDesc val.TupleDesc
	k, v       val.Tuple
	iter       prolly.MapIter
}

func (c *coveringLaxSecondaryLookup) New(ctx context.Context, k val.Tuple) error {
	// if no nil fields can do lookup
	var lax bool
	for i := 0; i < k.Count(); i++ {
		if k.FieldIsNull(i) {
			lax = true
			break
		}
	}

	if !lax {
		return c.m.Get(ctx, k, func(key val.Tuple, value val.Tuple) error {
			c.k = key
			c.v = value
			return nil
		})
	}

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

func (c *coveringLaxSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, more bool, err error) {
	if c.iter == nil {
		k, v, more = c.k, c.v, false
		c.k, c.v = nil, nil
		return
	}
	k, v, err = c.iter.Next(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, nil, false, err
		}
		c.iter = nil
	}
	return k, v, c.iter != nil, nil
}

type nonCoveringLaxSecondaryLookup struct {
	pri        prolly.Map
	sec        prolly.Map
	k, v       val.Tuple
	prefixDesc val.TupleDesc
	pkMap      val.OrdinalMapping
	pkBld      *val.TupleBuilder
	secIter    prolly.MapIter
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

func (c *nonCoveringLaxSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, more bool, err error) {
	if c.secIter == nil {
		// strict key already found
		k, v, more = c.k, c.v, false
		c.k, c.v = nil, nil
		return
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
	prefixDesc val.TupleDesc
	secIter    prolly.MapIter
	pkMap      val.OrdinalMapping
	pkBld      val.TupleBuilder

	k, v val.Tuple
	card uint64
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
}

func (c *keylessSecondaryLookup) Next(ctx context.Context) (k, v val.Tuple, more bool, err error) {
	if c.card > 0 {
		// exhaust duplicates
		c.card--
		k, v, more = c.k, c.v, c.card > 0
		return
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
