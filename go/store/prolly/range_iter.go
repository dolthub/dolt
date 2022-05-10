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

	"github.com/dolthub/dolt/go/store/skip"

	"github.com/dolthub/dolt/go/store/val"
)

type MapIter kvIter[val.Tuple, val.Tuple]

var _ MapIter = &mutableMapIter[val.Tuple, val.Tuple, val.TupleDesc]{}
var _ MapIter = &orderedTreeIter[val.Tuple, val.Tuple]{}

type rangeIter[K, V ~[]byte] interface {
	iterate(ctx context.Context) error
	current() (key K, value V)
}

var _ rangeIter[val.Tuple, val.Tuple] = &orderedTreeIter[val.Tuple, val.Tuple]{}
var _ rangeIter[val.Tuple, val.Tuple] = &memRangeIter{}
var _ rangeIter[val.Tuple, val.Tuple] = emptyIter{}

// mutableMapIter iterates over a Range of Tuples.
type mutableMapIter[K, V ~[]byte, O ordering[K]] struct {
	memory rangeIter[K, V]
	prolly *orderedTreeIter[K, V]
	order  O
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it mutableMapIter[K, V, O]) Next(ctx context.Context) (key K, value V, err error) {
	for {
		mk, mv := it.memory.current()
		pk, pv := it.prolly.current()

		if mk == nil && pk == nil {
			// range is exhausted
			return nil, nil, io.EOF
		}

		cmp := it.compareKeys(pk, mk)
		switch {
		case cmp < 0:
			key, value = pk, pv
			if err = it.prolly.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp > 0:
			key, value = mk, mv
			if err = it.memory.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp == 0:
			// |it.memory| wins ties
			key, value = mk, mv
			if err = it.memory.iterate(ctx); err != nil {
				return nil, nil, err
			}
			if err = it.prolly.iterate(ctx); err != nil {
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
		memKey, _ = it.memory.current()
	}
	if it.prolly != nil {
		proKey, _ = it.prolly.current()
	}
	return
}

func (it mutableMapIter[K, V, O]) compareKeys(memKey, proKey K) int {
	if memKey == nil {
		return 1
	}
	if proKey == nil {
		return -1
	}
	return it.order.Compare(memKey, proKey)
}

func memIterFromRange(list *skip.List, rng Range) *memRangeIter {
	var iter *skip.ListIter
	if rng.Start == nil {
		iter = list.IterAtStart()
	} else {
		// use the lower bound of |rng| to construct a skip.ListIter
		iter = list.GetIterFromSearchFn(skipSearchFromRange(rng))
	}

	// enforce range start
	var key val.Tuple
	for {
		key, _ = iter.Current()
		if key == nil || rng.AboveStart(key) {
			break // |i| inside |rng|
		}
		iter.Advance()
	}

	// enforce range end
	if key == nil || !rng.BelowStop(key) {
		iter = nil
	}

	return &memRangeIter{
		iter: iter,
		rng:  rng,
	}
}

// skipSearchFromRange is a skip.SearchFn used to initialize
// a skip.List iterator for a given Range. The skip.SearchFn
// returns true if the iter being initialized is not yet
// within the bounds of Range |rng|.
func skipSearchFromRange(rng Range) skip.SearchFn {
	return func(nodeKey []byte) bool {
		if nodeKey == nil {
			return false
		}
		return !rng.AboveStart(nodeKey)
	}
}

// todo(andy): generalize Range iteration and consolidate this
//  iterator with orderedListIter[K, V] in ordered_map.go.
//  This is not currently possible due to Range checking logic
//  that is specific to val.Tuple.
type memRangeIter struct {
	iter *skip.ListIter
	rng  Range
}

// current returns the iter's current Tuple pair, or nil Tuples
// if the iter has exhausted its range, it will
func (it *memRangeIter) current() (key, value val.Tuple) {
	// |it.iter| is set to nil when its range is exhausted
	if it.iter != nil {
		key, value = it.iter.Current()
	}
	return
}

// iterate progresses the iter inside its range.
func (it *memRangeIter) iterate(context.Context) (err error) {
	for {
		it.iter.Advance()

		k, _ := it.current()
		if k == nil || !it.rng.BelowStop(k) {
			it.iter = nil // range exhausted
		}

		return
	}
}

type emptyIter struct{}

func (e emptyIter) Next(context.Context) (val.Tuple, val.Tuple, error) {
	return nil, nil, io.EOF
}

func (e emptyIter) iterate(ctx context.Context) (err error) { return }

func (e emptyIter) current() (key, value val.Tuple) { return }
