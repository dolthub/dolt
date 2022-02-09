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
	"fmt"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Map struct {
	root    Node
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	ns      NodeStore
}

type KeyValueFn func(key, value val.Tuple) error

// NewMap creates an empty prolly tree Map
func NewMap(node Node, ns NodeStore, keyDesc, valDesc val.TupleDesc) Map {
	return Map{
		root:    node,
		keyDesc: keyDesc,
		valDesc: valDesc,
		ns:      ns,
	}
}

// NewMapFromTuples creates a prolly tree Map from slice of sorted Tuples.
func NewMapFromTuples(ctx context.Context, ns NodeStore, keyDesc, valDesc val.TupleDesc, tups ...val.Tuple) (Map, error) {
	m := NewMap(Node{}, ns, keyDesc, valDesc)

	ch, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	if err != nil {
		return Map{}, err
	}

	if len(tups)%2 != 0 {
		return Map{}, fmt.Errorf("tuples must be key-value pairs")
	}

	for i := 0; i < len(tups); i += 2 {
		_, err = ch.Append(ctx, nodeItem(tups[i]), nodeItem(tups[i+1]))
		if err != nil {
			return Map{}, err
		}
	}

	m.root, err = ch.Done(ctx)
	if err != nil {
		return Map{}, err
	}

	return m, nil
}

func DiffMaps(ctx context.Context, from, to Map, cb DiffFn) error {
	differ, err := treeDifferFromMaps(ctx, from, to)
	if err != nil {
		return err
	}

	for {
		var diff Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

// Mutate makes a MutableMap from a Map.
func (m Map) Mutate() MutableMap {
	return newMutableMap(m)
}

// todo(andy): support this?
//// Count returns the number of key-value pairs in the Map.
//func (m Map) Count() uint64 {
//	return m.root.cumulativeCount() / 2
//}

// HashOf returns the Hash of this Map.
func (m Map) HashOf() hash.Hash {
	return m.root.hashOf()
}

// Format returns the NomsBinFormat of this Map.
func (m Map) Format() *types.NomsBinFormat {
	return m.ns.Format()
}

// Descriptors returns the TupleDesc's from this Map.
func (m Map) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m Map) Empty() bool {
	return m.root.empty()
}

// Get searches for the key-value pair keyed by |key| and passes the results to the callback.
// If |key| is not present in the map, a nil key-value pair are passed.
func (m Map) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	cur, err := newLeafCursorAtItem(ctx, m.ns, m.root, nodeItem(key), m.searchNode)
	if err != nil {
		return err
	}

	var k, v val.Tuple
	if cur.valid() {
		k = val.Tuple(cur.currentKey())
		if m.compareKeys(key, k) == 0 {
			v = val.Tuple(cur.currentValue())
		} else {
			k = nil
		}
	}

	return cb(k, v)
}

// Has returns true is |key| is present in the Map.
func (m Map) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	cur, err := newLeafCursorAtItem(ctx, m.ns, m.root, nodeItem(key), m.searchNode)
	if err != nil {
		return false, err
	}

	if cur.valid() {
		k := val.Tuple(cur.currentKey())
		ok = m.compareKeys(key, k) == 0
	}

	return
}

// IterAll returns a MapRangeIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{
		Start:   RangeCut{Unbound: true},
		Stop:    RangeCut{Unbound: true},
		KeyDesc: m.keyDesc,
	}
	return m.IterRange(ctx, rng)
}

// IterRange returns a MapRangeIter that iterates over a Range.
func (m Map) IterRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	iter, err := m.iterFromRange(ctx, rng)
	if err != nil {
		return MapRangeIter{}, err
	}

	return NewMapRangeIter(nil, iter, rng), nil
}

func (m Map) iterFromRange(ctx context.Context, rng Range) (*prollyRangeIter, error) {
	var (
		err   error
		start *nodeCursor
		stop  *nodeCursor
	)

	startSearch := m.rangeStartSearchFn(rng)
	if rng.Start.Unbound {
		start, err = newCursorAtStart(ctx, m.ns, m.root)
	} else {
		start, err = newCursorAtTuple(ctx, m.ns, m.root, rng.Start.Key, startSearch)
	}
	if err != nil {
		return nil, err
	}

	stopSearch := m.rangeStopSearchFn(rng)
	if rng.Stop.Unbound {
		stop, err = newCursorPastEnd(ctx, m.ns, m.root)
	} else {
		stop, err = newCursorAtTuple(ctx, m.ns, m.root, rng.Stop.Key, stopSearch)
	}
	if err != nil {
		return nil, err
	}

	if start.compare(stop) >= 0 {
		start = nil // empty range
	}

	return &prollyRangeIter{
		curr: start,
		stop: stop,
	}, nil
}

func (m Map) rangeStartSearchFn(rng Range) searchFn {
	// todo(andy): inline sort.Search()
	return func(query nodeItem, nd Node) int {
		return sort.Search(nd.nodeCount(), func(i int) bool {
			q := val.Tuple(query)
			t := val.Tuple(nd.getKey(i))

			// compare using the range's tuple descriptor.
			cmp := rng.KeyDesc.Compare(q, t)
			if rng.Start.Inclusive {
				return cmp <= 0
			} else {
				return cmp < 0
			}
		})
	}
}

func (m Map) rangeStopSearchFn(rng Range) searchFn {
	// todo(andy): inline sort.Search()
	return func(query nodeItem, nd Node) int {
		return sort.Search(nd.nodeCount(), func(i int) bool {
			q := val.Tuple(query)
			t := val.Tuple(nd.getKey(i))

			// compare using the range's tuple descriptor.
			cmp := rng.KeyDesc.Compare(q, t)
			if rng.Stop.Inclusive {
				return cmp < 0
			} else {
				return cmp <= 0
			}
		})
	}
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func (m Map) searchNode(query nodeItem, nd Node) int {
	n := nd.nodeCount()
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := m.compareItems(query, nd.getKey(h)) <= 0
		// i ≤ h < j
		if !less {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and
	// f(j) (= f(i)) == true  =>  answer is i.
	return i
}

var _ searchFn = Map{}.searchNode

// compareItems is a compareFn.
func (m Map) compareItems(left, right nodeItem) int {
	l, r := val.Tuple(left), val.Tuple(right)
	return m.compareKeys(l, r)
}

func (m Map) compareKeys(left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}

type prollyRangeIter struct {
	// current tuple location
	curr *nodeCursor
	// non-inclusive range stop
	stop *nodeCursor
}

var _ rangeIter = &prollyRangeIter{}

func (it *prollyRangeIter) current() (key, value val.Tuple) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.valid() {
		key = val.Tuple(it.curr.currentKey())
		value = val.Tuple(it.curr.currentValue())
	}
	return
}

func (it *prollyRangeIter) iterate(ctx context.Context) (err error) {
	_, err = it.curr.advance(ctx)
	if err != nil {
		return err
	}

	if it.curr.compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}
