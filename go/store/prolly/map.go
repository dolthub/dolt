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
		if err = ch.AddPair(ctx, tups[i], tups[i+1]); err != nil {
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

// Count returns the number of key-value pairs in the Map.
func (m Map) Count() int {
	return m.root.treeCount()
}

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

func (m Map) Last(ctx context.Context) (key, value val.Tuple, err error) {
	cur, err := newCursorAtEnd(ctx, m.ns, m.root)
	if err != nil {
		return nil, nil, err
	}

	if cur.valid() {
		key = val.Tuple(cur.currentKey())
		value = val.Tuple(cur.currentValue())
	}
	return
}

// IterAll returns a MutableMapRangeIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{Start: nil, Stop: nil, Desc: m.keyDesc}
	return m.IterRange(ctx, rng)
}

// IterRange returns a MutableMapRangeIter that iterates over a Range.
func (m Map) IterRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	if rng.isPointLookup(m.keyDesc) {
		return m.pointLookupFromRange(ctx, rng)
	} else {
		return m.iterFromRange(ctx, rng)
	}
}

func (m Map) pointLookupFromRange(ctx context.Context, rng Range) (*pointLookup, error) {
	search := pointLookupSearchFn(rng)
	cur, err := newCursorFromSearchFn(ctx, m.ns, m.root, search)
	if err != nil {
		return nil, err
	}

	key := val.Tuple(cur.currentKey())
	value := val.Tuple(cur.currentValue())
	if compareBound(rng.Start, key, m.keyDesc) != 0 {
		// map does not contain this point lookup
		key, value = nil, nil
	}

	return &pointLookup{k: key, v: value}, nil
}

func (m Map) iterFromRange(ctx context.Context, rng Range) (*prollyRangeIter, error) {
	var (
		err   error
		start *nodeCursor
		stop  *nodeCursor
	)

	startSearch := rangeStartSearchFn(rng)
	if rng.Start == nil {
		start, err = newCursorAtStart(ctx, m.ns, m.root)
	} else {
		start, err = newCursorFromSearchFn(ctx, m.ns, m.root, startSearch)
	}
	if err != nil {
		return nil, err
	}

	stopSearch := rangeStopSearchFn(rng)
	if rng.Stop == nil {
		stop, err = newCursorPastEnd(ctx, m.ns, m.root)
	} else {
		stop, err = newCursorFromSearchFn(ctx, m.ns, m.root, stopSearch)
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

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func (m Map) searchNode(query nodeItem, nd Node) int {
	n := int(nd.count)
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

var _ itemSearchFn = Map{}.searchNode

// compareItems is a compareFn.
func (m Map) compareItems(left, right nodeItem) int {
	l, r := val.Tuple(left), val.Tuple(right)
	return m.compareKeys(l, r)
}

func (m Map) compareKeys(left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}
