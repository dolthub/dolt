// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

type MapIter tree.KvIter[val.Tuple, val.Tuple]

var _ MapIter = &mutableMapIter[val.Tuple, val.Tuple, val.TupleDesc]{}
var _ MapIter = &tree.OrderedTreeIter[val.Tuple, val.Tuple]{}

type rangeIter[K, V ~[]byte] interface {
	Iterate(ctx context.Context) error
	Current() (key K, value V)
}

var _ rangeIter[val.Tuple, val.Tuple] = &tree.OrderedTreeIter[val.Tuple, val.Tuple]{}
var _ rangeIter[val.Tuple, val.Tuple] = &memRangeIter{}
var _ rangeIter[val.Tuple, val.Tuple] = emptyIter{}

// mutableMapIter iterates over a Range of Tuples.
type mutableMapIter[K, V ~[]byte, O tree.Ordering[K]] struct {
	memory rangeIter[K, V]
	prolly *tree.OrderedTreeIter[K, V]
	order  O
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it mutableMapIter[K, V, O]) Next(ctx context.Context) (key K, value V, err error) {
	for {
		mk, mv := it.memory.Current()
		pk, pv := it.prolly.Current()

		if mk == nil && pk == nil {
			// range is exhausted
			return nil, nil, io.EOF
		}

		cmp := it.compareKeys(ctx, pk, mk)
		switch {
		case cmp < 0:
			key, value = pk, pv
			if err = it.prolly.Iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp > 0:
			key, value = mk, mv
			if err = it.memory.Iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp == 0:
			// |it.memory| wins ties
			key, value = mk, mv
			if err = it.memory.Iterate(ctx); err != nil {
				return nil, nil, err
			}
			if err = it.prolly.Iterate(ctx); err != nil {
				return nil, nil, err
			}
		}

		if key != nil && value == nil {
			continue // pending delete
		}

		return key, value, nil
	}
}

func (it mutableMapIter[K, V, O]) currentKeys() (memKey, proKey K) {
	if it.memory != nil {
		memKey, _ = it.memory.Current()
	}
	if it.prolly != nil {
		proKey, _ = it.prolly.Current()
	}
	return
}

func (it mutableMapIter[K, V, O]) compareKeys(ctx context.Context, memKey, proKey K) int {
	if memKey == nil {
		return 1
	}
	if proKey == nil {
		return -1
	}
	return it.order.Compare(ctx, memKey, proKey)
}

func memIterFromRange(ctx context.Context, list *skip.List, rng Range) *memRangeIter {
	// use the lower bound of |rng| to construct a skip.ListIter
	iter := list.GetIterFromSeekFn(skipSearchFromRange(ctx, rng))

	// enforce range start
	var key val.Tuple
	for {
		key, _ = iter.Current()
		if key == nil || rng.aboveStart(ctx, key) {
			break // |i| inside |rng|
		}
		iter.Advance()
	}

	// enforce range end
	if key == nil || !rng.belowStop(ctx, key) {
		iter = nil
	}

	return &memRangeIter{
		iter: iter,
		rng:  rng,
	}
}

// skipSearchFromRange is a skip.SeekFn used to initialize
// a skip.List iterator for a given Range. The skip.SearchFn
// returns true if the iter being initialized is not yet
// within the bounds of Range |rng|.
func skipSearchFromRange(ctx context.Context, rng Range) skip.SeekFn {
	return func(nodeKey []byte) bool {
		if nodeKey == nil {
			return false
		}
		return !rng.aboveStart(ctx, nodeKey)
	}
}

// todo(andy): generalize Range iteration and consolidate this
// iterator with orderedListIter[K, V] in ordered_map.go.
// This is not currently possible due to Range checking logic
// that is specific to val.Tuple.
type memRangeIter struct {
	iter *skip.ListIter
	rng  Range
}

// Current returns the iter's current Tuple pair, or nil Tuples
// if the iter has exhausted its range, it will
func (it *memRangeIter) Current() (key, value val.Tuple) {
	// |it.iter| is set to nil when its range is exhausted
	if it.iter != nil {
		key, value = it.iter.Current()
	}
	return
}

// Iterate progresses the iter inside its range.
func (it *memRangeIter) Iterate(ctx context.Context) (err error) {
	for {
		it.iter.Advance()

		k, _ := it.Current()
		if k == nil || !it.rng.belowStop(ctx, k) {
			it.iter = nil // range exhausted
		}

		return
	}
}

type emptyIter struct{}

func (e emptyIter) Next(context.Context) (val.Tuple, val.Tuple, error) {
	return nil, nil, io.EOF
}

func (e emptyIter) Iterate(ctx context.Context) (err error) { return }

func (e emptyIter) Current() (key, value val.Tuple) { return }

type filteredIter struct {
	iter MapIter
	rng  Range
}

var _ MapIter = filteredIter{}

func (f filteredIter) Next(ctx context.Context) (k, v val.Tuple, err error) {
	for {
		k, v, err = f.iter.Next(ctx)
		if err != nil {
			return nil, nil, err
		}
		if !f.rng.Matches(ctx, k) {
			continue
		}
		return
	}
}
