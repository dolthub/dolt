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
	"io"
	"strings"

	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Map struct {
	root    tree.Node
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
	ns      tree.NodeStore
}

type KeyValueFn func(key, value val.Tuple) error

type DiffFn func(context.Context, tree.Diff) error

// NewMap creates an empty prolly tree Map
func NewMap(node tree.Node, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc) Map {
	return Map{
		root:    node,
		keyDesc: keyDesc,
		valDesc: valDesc,
		ns:      ns,
	}
}

// NewMapFromTuples creates a prolly tree Map from slice of sorted Tuples.
func NewMapFromTuples(ctx context.Context, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc, tups ...val.Tuple) (Map, error) {
	m := NewMap(tree.Node{}, ns, keyDesc, valDesc)

	ch, err := tree.NewEmptyChunker(ctx, ns)
	if err != nil {
		return Map{}, err
	}

	if len(tups)%2 != 0 {
		return Map{}, fmt.Errorf("tuples must be key-value pairs")
	}

	for i := 0; i < len(tups); i += 2 {
		if err = ch.AddPair(ctx, tree.NodeItem(tups[i]), tree.NodeItem(tups[i+1])); err != nil {
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
	differ, err := tree.DifferFromRoots(ctx, from.ns, from.root, to.root, to.compareItems)
	if err != nil {
		return err
	}

	for {
		var diff tree.Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

func MergeMaps(ctx context.Context, left, right, base Map, cb tree.CollisionFn) (Map, error) {
	sfn, cfn := base.searchNode, base.compareItems

	merged, err := tree.ThreeWayMerge(ctx, base.ns, left.root, right.root, base.root, sfn, cfn, cb)
	if err != nil {
		return Map{}, err
	}

	return Map{
		root:    merged,
		keyDesc: base.keyDesc,
		valDesc: base.valDesc,
		ns:      base.ns,
	}, nil
}

// Mutate makes a MutableMap from a Map.
func (m Map) Mutate() MutableMap {
	return newMutableMap(m)
}

// Count returns the number of key-value pairs in the Map.
func (m Map) Count() int {
	return m.root.TreeCount()
}

func (m Map) Height() int {
	return m.root.Level() + 1
}

// HashOf returns the Hash of this Map.
func (m Map) HashOf() hash.Hash {
	return m.root.HashOf()
}

// Format returns the NomsBinFormat of this Map.
func (m Map) Format() *types.NomsBinFormat {
	return m.ns.Format()
}

// Descriptors returns the TupleDesc's from this Map.
func (m Map) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m Map) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return tree.WalkAddresses(ctx, m.root, m.ns, cb)
}

func (m Map) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return tree.WalkNodes(ctx, m.root, m.ns, cb)
}

// Get searches for the key-value pair keyed by |key| and passes the results to the callback.
// If |key| is not present in the map, a nil key-value pair are passed.
func (m Map) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, m.ns, m.root, tree.NodeItem(key), m.searchNode)
	if err != nil {
		return err
	}

	var k, v val.Tuple
	if cur.Valid() {
		k = val.Tuple(cur.CurrentKey())
		if m.compareKeys(key, k) == 0 {
			v = val.Tuple(cur.CurrentValue())
		} else {
			k = nil
		}
	}

	return cb(k, v)
}

// Has returns true is |key| is present in the Map.
func (m Map) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, m.ns, m.root, tree.NodeItem(key), m.searchNode)
	if err != nil {
		return false, err
	}

	if cur.Valid() {
		k := val.Tuple(cur.CurrentKey())
		ok = m.compareKeys(key, k) == 0
	}

	return
}

func (m Map) Last(ctx context.Context) (key, value val.Tuple, err error) {
	cur, err := tree.NewCursorAtEnd(ctx, m.ns, m.root)
	if err != nil {
		return nil, nil, err
	}

	if cur.Valid() {
		key = val.Tuple(cur.CurrentKey())
		value = val.Tuple(cur.CurrentValue())
	}
	return
}

// IterAll returns a MutableMapRangeIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{Start: nil, Stop: nil, Desc: m.keyDesc}
	return m.IterRange(ctx, rng)
}

// IterOrdinalRange returns a MapRangeIter for the ordinal range beginning at |start| and ending before |stop|.
func (m Map) IterOrdinalRange(ctx context.Context, start, stop uint64) (MapRangeIter, error) {
	if stop == start {
		return emptyIter{}, nil
	} else if stop < start {
		return nil, fmt.Errorf("invalid ordinal bounds (%d, %d)", start, stop)
	} else if stop > uint64(m.Count()) {
		return nil, fmt.Errorf("stop index (%d) out of bounds", stop)
	}

	lo, err := tree.NewCursorAtOrdinal(ctx, m.ns, m.root, start)
	if err != nil {
		return nil, err
	}

	hi, err := tree.NewCursorAtOrdinal(ctx, m.ns, m.root, stop)
	if err != nil {
		return nil, err
	}

	return &prollyRangeIter{
		curr: lo,
		stop: hi,
	}, nil
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
	cur, err := tree.NewCursorFromSearchFn(ctx, m.ns, m.root, search)
	if err != nil {
		return nil, err
	}
	if !cur.Valid() {
		// map does not contain |rng|
		return &pointLookup{}, nil
	}

	key := val.Tuple(cur.CurrentKey())
	value := val.Tuple(cur.CurrentValue())
	if compareBound(rng.Start, key, m.keyDesc) != 0 {
		// map does not contain |rng|
		return &pointLookup{}, nil
	}

	return &pointLookup{k: key, v: value}, nil
}

func (m Map) iterFromRange(ctx context.Context, rng Range) (*prollyRangeIter, error) {
	var (
		err   error
		start *tree.Cursor
		stop  *tree.Cursor
	)

	startSearch := rangeStartSearchFn(rng)
	if rng.Start == nil {
		start, err = tree.NewCursorAtStart(ctx, m.ns, m.root)
	} else {
		start, err = tree.NewCursorFromSearchFn(ctx, m.ns, m.root, startSearch)
	}
	if err != nil {
		return nil, err
	}

	stopSearch := rangeStopSearchFn(rng)
	if rng.Stop == nil {
		stop, err = tree.NewCursorPastEnd(ctx, m.ns, m.root)
	} else {
		stop, err = tree.NewCursorFromSearchFn(ctx, m.ns, m.root, stopSearch)
	}
	if err != nil {
		return nil, err
	}

	if start.Compare(stop) >= 0 {
		start = nil // empty range
	}

	return &prollyRangeIter{
		curr: start,
		stop: stop,
	}, nil
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func (m Map) searchNode(query tree.NodeItem, nd tree.Node) int {
	n := int(nd.Count())
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := m.compareItems(query, nd.GetKey(h)) <= 0
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

var _ tree.ItemSearchFn = Map{}.searchNode

// compareItems is a CompareFn.
func (m Map) compareItems(left, right tree.NodeItem) int {
	l, r := val.Tuple(left), val.Tuple(right)
	return m.compareKeys(l, r)
}

func (m Map) compareKeys(left, right val.Tuple) int {
	return int(m.keyDesc.Compare(left, right))
}

// DebugFormat formats a Map.
func DebugFormat(ctx context.Context, m Map) (string, error) {
	kd, vd := m.Descriptors()
	iter, err := m.IterAll(ctx)
	if err != nil {
		return "", err
	}
	c := m.Count()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Prolly Map (count: %d) {\n", c))
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		sb.WriteString("\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(v))
		sb.WriteString(",\n")
	}
	sb.WriteString("}")
	return sb.String(), nil
}
