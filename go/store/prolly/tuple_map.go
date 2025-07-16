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
	tuples  tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

// NewMap creates a Map from the supplied root node
func NewMap(node tree.Node, ns tree.NodeStore, keyDesc, valDesc val.TupleDesc) Map {
	tuples := tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:      node,
		NodeStore: ns,
		Order:     keyDesc,
	}
	return Map{
		tuples:  tuples,
		keyDesc: keyDesc,
		valDesc: valDesc,
	}
}

// NewMapFromTuples creates a prolly Tree Map from slice of sorted Tuples.
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
	fn := tree.ApplyMutations[val.Tuple, val.TupleDesc, message.ProllyMapSerializer]
	s := message.NewProllyMapSerializer(m.valDesc, m.tuples.NodeStore.Pool())

	root, err := fn(ctx, m.tuples.NodeStore, m.tuples.Root, m.keyDesc, s, mutationIter{iter: iter})
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples: tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
			Root:      root,
			NodeStore: m.tuples.NodeStore,
			Order:     m.tuples.Order,
		},
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}, nil
}

func DiffMaps(ctx context.Context, from, to Map, considerAllRowsModified bool, cb tree.DiffFn) error {
	return tree.DiffOrderedTrees(ctx, from.tuples, to.tuples, considerAllRowsModified, makeDiffCallBack(from, to, cb))
}

// RangeDiffMaps returns diffs within a Range. See Range for which diffs are
// returned.
func RangeDiffMaps(ctx context.Context, from, to Map, rng Range, cb tree.DiffFn) error {
	differ, err := tree.DifferFromCursors[val.Tuple, val.TupleDesc](
		ctx, from.tuples.Root, to.tuples.Root,
		rangeStartSearchFn(rng), rangeStopSearchFn(rng),
		from.NodeStore(), to.NodeStore(),
		from.tuples.Order,
	)
	if err != nil {
		return err
	}

	dcb := makeDiffCallBack(from, to, cb)

	for {
		var diff tree.Diff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = dcb(ctx, diff); err != nil {
			break
		}
	}
	return err
}

// DiffMapsKeyRange returns diffs within a physical key range. The key range is
// specified by |start| and |stop|. If |start| and/or |stop| is null, then the
// range is unbounded towards that end.
func DiffMapsKeyRange(ctx context.Context, from, to Map, start, stop val.Tuple, cb tree.DiffFn) error {
	return tree.DiffKeyRangeOrderedTrees(ctx, from.tuples, to.tuples, start, stop, makeDiffCallBack(from, to, cb))
}

func makeDiffCallBack(from, to Map, innerCb tree.DiffFn) tree.DiffFn {
	if !from.valDesc.Equals(to.valDesc) {
		return innerCb
	}

	return func(ctx context.Context, diff tree.Diff) error {
		// Skip diffs produced by non-canonical tuples. A canonical-tuple is a
		// tuple where any null suffixes have been trimmed.
		if diff.Type == tree.ModifiedDiff &&
			from.valDesc.Compare(ctx, val.Tuple(diff.From), val.Tuple(diff.To)) == 0 {
			return nil
		}
		return innerCb(ctx, diff)
	}
}

func MergeMaps(ctx context.Context, left, right, base Map, cb tree.CollisionFn) (Map, tree.MergeStats, error) {
	serializer := message.NewProllyMapSerializer(left.valDesc, base.NodeStore().Pool())
	// TODO: MergeMaps does not properly detect merge conflicts when one side adds a NULL to the end of its tuple.
	// However, since `MergeMaps` is not currently called, fixing this is not a priority.
	tuples, stats, err := tree.MergeOrderedTrees(ctx, left.tuples, right.tuples, base.tuples, cb, serializer)
	if err != nil {
		return Map{}, tree.MergeStats{}, err
	}

	return Map{
		tuples:  tuples,
		keyDesc: base.keyDesc,
		valDesc: base.valDesc,
	}, stats, nil
}

// VisitMapLevelOrder visits each internal node of the tree in level order and calls the provided callback `cb` on each hash
// encountered. This function is used primarily for building appendix table files for databases to help optimize reads.
func VisitMapLevelOrder(ctx context.Context, m Map, cb func(h hash.Hash) (int64, error)) error {
	return tree.VisitMapLevelOrder(ctx, m.tuples, cb)
}

// NodeStore returns the map's NodeStore
func (m Map) NodeStore() tree.NodeStore {
	return m.tuples.NodeStore
}

func (m Map) Tuples() tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc] {
	return m.tuples
}

func (m Map) ValDesc() val.TupleDesc {
	return m.valDesc
}

func (m Map) KeyDesc() val.TupleDesc {
	return m.keyDesc
}

// Mutate makes a MutableMap from a Map.
func (m Map) Mutate() *MutableMap {
	return newMutableMap(m)
}

// MutateInterface makes a MutableMap from a Map.
func (m Map) MutateInterface() MutableMapInterface {
	return newMutableMap(m)
}

// Rewriter returns a mutator that intends to rewrite this map with the key and value descriptors provided.
func (m Map) Rewriter(kd, vd val.TupleDesc) *MutableMap {
	return newMutableMapWithDescriptors(m, kd, vd)
}

// Count returns the number of key-value pairs in the Map.
func (m Map) Count() (int, error) {
	return m.tuples.Count()
}

func (m Map) Height() int {
	return m.tuples.Height()
}

// HashOf returns the Hash of this Map.
func (m Map) HashOf() hash.Hash {
	return m.tuples.HashOf()
}

// Format returns the NomsBinFormat of this Map.
func (m Map) Format() *types.NomsBinFormat {
	return m.tuples.NodeStore.Format()
}

// Descriptors returns the TupleDesc's from this Map.
func (m Map) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m Map) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return m.tuples.WalkAddresses(ctx, cb)
}

func (m Map) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return m.tuples.WalkNodes(ctx, cb)
}

// Get searches for the key-value pair keyed by |key| and passes the results to the callback.
// If |key| is not present in the map, a nil key-value pair are passed.
func (m Map) Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.Get(ctx, key, cb)
}

// GetPrefix returns the first key-value pair that matches the prefix key
// or nil to the callback.
func (m Map) GetPrefix(ctx context.Context, key val.Tuple, prefDesc val.TupleDesc, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.GetPrefix(ctx, key, prefDesc, cb)
}

// todo(andy): iter prefix

// Has returns true is |key| is present in the Map.
func (m Map) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return m.tuples.Has(ctx, key)
}

func (m Map) LastKey(ctx context.Context) val.Tuple {
	return m.tuples.LastKey(ctx)
}

// IterAll returns a MapIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapIter, error) {
	return m.tuples.IterAll(ctx)
}

// IterAllReverse returns a MapIter that iterates over the entire Map from the end to the beginning.
func (m Map) IterAllReverse(ctx context.Context) (MapIter, error) {
	return m.tuples.IterAllReverse(ctx)
}

// IterOrdinalRange returns a MapIter for the ordinal range beginning at |start| and ending before |stop|.
func (m Map) IterOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error) {
	return m.tuples.IterOrdinalRange(ctx, start, stop)
}

// FetchOrdinalRange fetches all leaf Nodes for the ordinal range beginning at |start|
// and ending before |stop| and returns an iterator over their Items.
func (m Map) FetchOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error) {
	return m.tuples.FetchOrdinalRange(ctx, start, stop)
}

// HasPrefix returns true if the Map contains any key matching |preKey|.
func (m Map) HasPrefix(ctx context.Context, preKey val.Tuple, preDesc val.TupleDesc) (bool, error) {
	// todo(andy): we should compute our own |prefixDesc| here, but
	//  we can't do that efficiently with the current TupleDesc.
	if preKey.Count() < preDesc.Count() {
		return false, fmt.Errorf("invalid prefix key (%d < %d)", preKey.Count(), preDesc.Count())
	} else if m.keyDesc.Count() < preDesc.Count() {
		return false, fmt.Errorf("invalid TupleDesc prefix (%d < %d)", m.keyDesc.Count(), preDesc.Count())
	}
	return m.tuples.HasPrefix(ctx, preKey, preDesc)
}

// IterRange returns a mutableMapIter that iterates over a Range.
func (m Map) IterRange(ctx context.Context, rng Range) (iter MapIter, err error) {
	stop, ok, err := rng.KeyRangeLookup(ctx, m.Pool(), m.NodeStore())
	if err != nil {
		return nil, err
	}
	if ok {
		iter, err = m.IterKeyRange(ctx, rng.Tup, stop)
	} else {
		iter, err = treeIterFromRange(ctx, m.tuples.Root, m.tuples.NodeStore, rng)
	}
	if err != nil {
		return nil, err
	}
	if !rng.SkipRangeMatchCallback || !rng.IsContiguous {
		// range.Matches check is required if a type is imprecise
		// or a key range is non-contiguous on disk
		iter = filteredIter{iter: iter, rng: rng}
	}
	return iter, nil
}

// IterRangeReverse returns a mutableMapIter that iterates over a Range backwards.
func (m Map) IterRangeReverse(ctx context.Context, rng Range) (MapIter, error) {
	iter, err := treeIterFromRangeReverse(ctx, m.tuples.Root, m.tuples.NodeStore, rng)
	if err != nil {
		return nil, err
	}
	return filteredIter{iter: iter, rng: rng}, nil
}

// IterKeyRange iterates over a physical key range defined by |start| and
// |stop|. If |startInclusive| and/or |stop| is nil, the range will be open
// towards that end.
func (m Map) IterKeyRange(ctx context.Context, start, stop val.Tuple) (MapIter, error) {
	return m.tuples.IterKeyRange(ctx, start, stop)
}

// GetOrdinalForKey returns the smallest ordinal position at which the key >=
// |query|.
func (m Map) GetOrdinalForKey(ctx context.Context, query val.Tuple) (uint64, error) {
	return m.tuples.GetOrdinalForKey(ctx, query)
}

// GetKeyRangeCardinality returns the number of key-value tuples between |start|
// and |stopExclusive|. If |start| and/or |stop| is null that end is unbounded.
func (m Map) GetKeyRangeCardinality(ctx context.Context, startInclusive val.Tuple, endExclusive val.Tuple) (uint64, error) {
	return m.tuples.GetKeyRangeCardinality(ctx, startInclusive, endExclusive)
}

func (m Map) Node() tree.Node {
	return m.tuples.Root
}

// Pool returns the pool.BuffPool of the underlying tuples' Tree.NodeStore
func (m Map) Pool() pool.BuffPool {
	return m.tuples.NodeStore.Pool()
}

func (m Map) CompareItems(ctx context.Context, left, right tree.Item) int {
	return m.keyDesc.Compare(ctx, val.Tuple(left), val.Tuple(right))
}

func treeIterFromRange(
	ctx context.Context,
	root tree.Node,
	ns tree.NodeStore,
	rng Range,
) (*tree.OrderedTreeIter[val.Tuple, val.Tuple], error) {
	findStart, findStop := rangeStartSearchFn(rng), rangeStopSearchFn(rng)
	return tree.OrderedTreeIterFromCursors[val.Tuple, val.Tuple](ctx, root, ns, findStart, findStop)
}

func treeIterFromRangeReverse(
	ctx context.Context,
	root tree.Node,
	ns tree.NodeStore,
	rng Range,
) (*tree.OrderedTreeIter[val.Tuple, val.Tuple], error) {
	findStart, findStop := rangeStartSearchFn(rng), rangeStopSearchFn(rng)
	return tree.ReverseOrderedTreeIterFromCursors[val.Tuple, val.Tuple](ctx, root, ns, findStart, findStop)
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
		sb.WriteString(kd.Format(ctx, k))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(ctx, v))
		sb.WriteString(",\n")
	}
	sb.WriteString("}")
	return sb.String(), nil
}

func AddHashToSchema(keyDesc val.TupleDesc) val.TupleDesc {
	newTypes := make([]val.Type, len(keyDesc.Types)+1)
	handlers := make([]val.TupleTypeHandler, len(keyDesc.Types)+1)
	copy(newTypes, keyDesc.Types)
	copy(handlers, keyDesc.Handlers)
	newTypes[len(newTypes)-1] = val.Type{Enc: val.Hash128Enc}
	handlers[len(handlers)-1] = nil
	return val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{
		Comparator: keyDesc.Comparator(),
		Handlers:   handlers,
	}, newTypes...)
}
