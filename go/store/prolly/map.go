// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Map struct {
	tuples  orderedTree[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

type DiffFn func(context.Context, tree.Diff) error

type DiffSummary struct {
	Adds, Removes        uint64
	Changes, CellChanges uint64
	NewSize, OldSize     uint64
}

// NewMap creates an empty prolly tree Map
func NewMap(node tree.Node, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc) Map {
	tuples := orderedTree[val.Tuple, val.Tuple, val.TupleDesc]{
		root:  node,
		ns:    ns,
		order: keyDesc,
	}
	return Map{
		tuples:  tuples,
		keyDesc: keyDesc,
		valDesc: valDesc,
	}
}

// NewMapFromTuples creates a prolly tree Map from slice of sorted Tuples.
func NewMapFromTuples(ctx context.Context, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc, tups ...val.Tuple) (Map, error) {
	if len(tups)%2 != 0 {
		return Map{}, fmt.Errorf("tuples must be key-value pairs")
	}

	return NewMapFromTupleIter(ctx, ns, keyDesc, valDesc, &tupleIter{tuples: tups})
}

type TupleIter interface {
	Next(ctx context.Context) (k, v val.Tuple)
}

func NewMapFromTupleIter(ctx context.Context, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc, iter TupleIter) (Map, error) {
	serializer := message.NewProllyMapSerializer(valDesc, ns.Pool())
	ch, err := tree.NewEmptyChunker(ctx, ns, serializer)
	if err != nil {
		return Map{}, err
	}

	var k, v val.Tuple
	for {
		k, v = iter.Next(ctx)
		if k == nil {
			break
		}
		if err != nil {
			return Map{}, err
		}
		if err = ch.AddPair(ctx, tree.Item(k), tree.Item(v)); err != nil {
			return Map{}, err
		}
	}

	root, err := ch.Done(ctx)
	if err != nil {
		return Map{}, err
	}

	return NewMap(root, ns, keyDesc, valDesc), nil
}

func MutateMapWithTupleIter(ctx context.Context, m Map, iter TupleIter) (Map, error) {
	t := m.tuples
	i := mutationIter{iter: iter}
	s := message.NewProllyMapSerializer(m.valDesc, t.ns.Pool())

	root, err := tree.ApplyMutations(ctx, t.ns, t.root, s, i, t.compareItems)
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples: orderedTree[val.Tuple, val.Tuple, val.TupleDesc]{
			root:  root,
			ns:    t.ns,
			order: t.order,
		},
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}, nil
}

func DiffMaps(ctx context.Context, from, to Map, cb DiffFn) error {
	return diffOrderedTrees(ctx, from.tuples, to.tuples, cb)
}

// RangeDiffMaps returns diffs within a Range. See Range for which diffs are
// returned.
func RangeDiffMaps(ctx context.Context, from, to Map, rng Range, cb DiffFn) error {
	return rangeDiffOrderedTrees(ctx, from.tuples, to.tuples, rng, cb)
}

// DiffKeyRangeMaps returns diffs within a physical key range. The key range is
// specified by |startInclusive| and |stopExclusive|. If |startInclusive| or
// |stopExclusive| is null, then the range is unbounded towards that end.
func DiffKeyRangeMaps(ctx context.Context, from, to Map, startInclusive, stopExclusive val.Tuple, cb DiffFn) error {
	return diffKeyRangeOrderedTrees(ctx, from.tuples, to.tuples, startInclusive, stopExclusive, cb)
}

func MergeMaps(ctx context.Context, left, right, base Map, cb tree.CollisionFn) (Map, error) {
	serializer := message.NewProllyMapSerializer(left.valDesc, base.NodeStore().Pool())
	tuples, err := mergeOrderedTrees(ctx, left.tuples, right.tuples, base.tuples, cb, serializer, base.valDesc)
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples:  tuples,
		keyDesc: base.keyDesc,
		valDesc: base.valDesc,
	}, nil
}

// NodeStore returns the map's NodeStore
func (m Map) NodeStore() tree.NodeStore {
	return m.tuples.ns
}

// Mutate makes a MutableMap from a Map.
func (m Map) Mutate() MutableMap {
	return newMutableMap(m)
}

// Count returns the number of key-value pairs in the Map.
func (m Map) Count() (int, error) {
	return m.tuples.count()
}

func (m Map) Height() (int, error) {
	return m.tuples.height()
}

// HashOf returns the Hash of this Map.
func (m Map) HashOf() hash.Hash {
	return m.tuples.hashOf()
}

// Format returns the NomsBinFormat of this Map.
func (m Map) Format() *types.NomsBinFormat {
	return m.tuples.ns.Format()
}

// Descriptors returns the TupleDesc's from this Map.
func (m Map) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m Map) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return m.tuples.walkAddresses(ctx, cb)
}

func (m Map) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return m.tuples.walkNodes(ctx, cb)
}

// Get searches for the key-value pair keyed by |key| and passes the results to the callback.
// If |key| is not present in the map, a nil key-value pair are passed.
func (m Map) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.get(ctx, key, cb)
}

// Has returns true is |key| is present in the Map.
func (m Map) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return m.tuples.has(ctx, key)
}

func (m Map) Last(ctx context.Context) (key, value val.Tuple, err error) {
	return m.tuples.last(ctx)
}

// IterAll returns a MapIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapIter, error) {
	return m.tuples.iterAll(ctx)
}

// IterAllReverse returns a MapIter that iterates over the entire Map from the end to the beginning.
func (m Map) IterAllReverse(ctx context.Context) (MapIter, error) {
	return m.tuples.iterAllReverse(ctx)
}

// IterOrdinalRange returns a MapIter for the ordinal range beginning at |start| and ending before |stop|.
func (m Map) IterOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error) {
	return m.tuples.iterOrdinalRange(ctx, start, stop)
}

// FetchOrdinalRange fetches all leaf Nodes for the ordinal range beginning at |start|
// and ending before |stop| and returns an iterator over their Items.
func (m Map) FetchOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error) {
	return m.tuples.fetchOrdinalRange(ctx, start, stop)
}

// IterRange returns a mutableMapIter that iterates over a Range.
func (m Map) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	if rng.IsPointLookup(m.keyDesc) {
		return m.pointLookupFromRange(ctx, rng)
	}

	iter, err := treeIterFromRange(ctx, m.tuples.root, m.tuples.ns, rng)
	if err != nil {
		return nil, err
	}
	return filteredIter{iter: iter, rng: rng}, nil
}

// IterKeyRange iterates over a physical key range defined by |startInclusive|
// and |stopExclusive|. If |startInclusive| and/or |stopExclusive| is nil, the
// range will be open towards that end.
func (m Map) IterKeyRange(ctx context.Context, startInclusive, stopExclusive val.Tuple) (MapIter, error) {
	return m.tuples.iterKeyRange(ctx, startInclusive, stopExclusive)
}

// GetOrdinal returns the smallest ordinal position at which the key >= |query|.
func (m Map) GetOrdinal(ctx context.Context, query val.Tuple) (uint64, error) {
	return m.tuples.getOrdinal(ctx, query)
}

// GetRangeCardinality returns the number of key-value tuples in |rng|.
func (m Map) GetRangeCardinality(ctx context.Context, rng Range) (uint64, error) {
	start, err := tree.NewCursorFromSearchFn(ctx, m.tuples.ns, m.tuples.root, rangeStartSearchFn(rng))
	if err != nil {
		return 0, err
	}

	stop, err := tree.NewCursorFromSearchFn(ctx, m.tuples.ns, m.tuples.root, rangeStopSearchFn(rng))
	if err != nil {
		return 0, err
	}

	stopF := func(curr *tree.Cursor) bool {
		return curr.Compare(stop) >= 0
	}

	if stopF(start) {
		return 0, nil // empty range
	}

	startOrd, err := tree.GetOrdinalOfCursor(start)
	if err != nil {
		return 0, err
	}

	endOrd, err := tree.GetOrdinalOfCursor(stop)
	if err != nil {
		return 0, err
	}

	return endOrd - startOrd, nil
}

// GetKeyRangeCardinality returns the number of key-value tuples between
// |startInclusive| and |stopExclusive|. If |startInclusive| and/or
// |endExclusive| is null that end is unbounded.
func (m Map) GetKeyRangeCardinality(ctx context.Context, startInclusive val.Tuple, endExclusive val.Tuple) (uint64, error) {
	return m.tuples.getKeyRangeCardinality(ctx, startInclusive, endExclusive)
}

func (m Map) Node() tree.Node {
	return m.tuples.root
}

// Pool returns the pool.BuffPool of the underlying tuples' tree.NodeStore
func (m Map) Pool() pool.BuffPool {
	return m.tuples.ns.Pool()
}

func (m Map) CompareItems(left, right tree.Item) int {
	return m.keyDesc.Compare(val.Tuple(left), val.Tuple(right))
}

func (m Map) pointLookupFromRange(ctx context.Context, rng Range) (*pointLookup, error) {
	cur, err := tree.NewCursorFromSearchFn(ctx, m.tuples.ns, m.tuples.root, rangeStartSearchFn(rng))
	if err != nil {
		return nil, err
	}
	if !cur.Valid() {
		// map does not contain |rng|
		return &pointLookup{}, nil
	}

	key := val.Tuple(cur.CurrentKey())
	value := val.Tuple(cur.CurrentValue())

	if !rng.Matches(key) {
		return &pointLookup{}, nil
	}

	return &pointLookup{k: key, v: value}, nil
}

func treeIterFromRange(
	ctx context.Context,
	root tree.Node,
	ns tree.NodeStore,
	rng Range,
) (*orderedTreeIter[val.Tuple, val.Tuple], error) {
	var (
		err   error
		start *tree.Cursor
		stop  *tree.Cursor
	)

	start, err = tree.NewCursorFromSearchFn(ctx, ns, root, rangeStartSearchFn(rng))
	if err != nil {
		return nil, err
	}

	stop, err = tree.NewCursorFromSearchFn(ctx, ns, root, rangeStopSearchFn(rng))
	if err != nil {
		return nil, err
	}

	stopF := func(curr *tree.Cursor) bool {
		return curr.Compare(stop) >= 0
	}

	if stopF(start) {
		start = nil // empty range
	}

	return &orderedTreeIter[val.Tuple, val.Tuple]{curr: start, stop: stopF, step: start.Advance}, nil
}

func NewPointLookup(k, v val.Tuple) *pointLookup {
	return &pointLookup{k, v}
}

var EmptyPointLookup = &pointLookup{}

type pointLookup struct {
	k, v val.Tuple
}

var _ MapIter = &pointLookup{}

func (p *pointLookup) Next(context.Context) (key, value val.Tuple, err error) {
	if p.k == nil || p.v == nil {
		err = io.EOF
	} else {
		key, value = p.k, p.v
		p.k, p.v = nil, nil
	}
	return
}

// DebugFormat formats a Map.
func DebugFormat(ctx context.Context, m Map) (string, error) {
	kd, vd := m.Descriptors()
	iter, err := m.IterAll(ctx)
	if err != nil {
		return "", err
	}
	c, err := m.Count()
	if err != nil {
		return "", err
	}

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

// ConvertToSecondaryKeylessIndex converts the given map to a keyless index map.
func ConvertToSecondaryKeylessIndex(m Map) Map {
	keyDesc, valDesc := m.Descriptors()
	newTypes := make([]val.Type, len(keyDesc.Types)+1)
	copy(newTypes, keyDesc.Types)
	newTypes[len(newTypes)-1] = val.Type{Enc: val.Hash128Enc}
	newKeyDesc := val.NewTupleDescriptorWithComparator(keyDesc.Comparator(), newTypes...)
	newTuples := m.tuples
	newTuples.order = newKeyDesc
	return Map{
		tuples:  newTuples,
		keyDesc: newKeyDesc,
		valDesc: valDesc,
	}
}
