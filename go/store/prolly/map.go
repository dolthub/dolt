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

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
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
	ch, err := tree.NewEmptyChunker(ctx, ns, newMapBuilder)
	if err != nil {
		return Map{}, err
	}

	if len(tups)%2 != 0 {
		return Map{}, fmt.Errorf("tuples must be key-value pairs")
	}

	for i := 0; i < len(tups); i += 2 {
		if err = ch.AddPair(ctx, tree.Item(tups[i]), tree.Item(tups[i+1])); err != nil {
			return Map{}, err
		}
	}

	root, err := ch.Done(ctx)
	if err != nil {
		return Map{}, err
	}

	return NewMap(root, ns, keyDesc, valDesc), nil
}

func DiffMaps(ctx context.Context, from, to Map, cb DiffFn) error {
	return diffOrderedTrees(ctx, from.tuples, to.tuples, cb)
}

func MergeMaps(ctx context.Context, left, right, base Map, cb tree.CollisionFn) (Map, error) {
	tuples, err := mergeOrderedTrees(ctx, left.tuples, right.tuples, base.tuples, cb)
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples:  tuples,
		keyDesc: base.keyDesc,
		valDesc: base.valDesc,
	}, nil
}

// Mutate makes a MutableMap from a Map.
func (m Map) Mutate() MutableMap {
	return newMutableMap(m)
}

// Count returns the number of key-value pairs in the Map.
func (m Map) Count() int {
	return m.tuples.count()
}

func (m Map) Height() int {
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

// IterAll returns a mutableMapIter that iterates over the entire Map.
func (m Map) IterAll(ctx context.Context) (MapIter, error) {
	return m.tuples.iterAll(ctx)
}

// IterOrdinalRange returns a MapIter for the ordinal range beginning at |start| and ending before |stop|.
func (m Map) IterOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error) {
	return m.tuples.iterOrdinalRange(ctx, start, stop)
}

// IterRange returns a mutableMapIter that iterates over a Range.
func (m Map) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	if rng.isPointLookup(m.keyDesc) {
		return m.pointLookupFromRange(ctx, rng)
	} else {
		return m.iterFromRange(ctx, rng)
	}
}

func (m Map) pointLookupFromRange(ctx context.Context, rng Range) (*pointLookup, error) {
	search := pointLookupSearchFn(rng)
	cur, err := tree.NewCursorFromSearchFn(ctx, m.tuples.ns, m.tuples.root, search)
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

func (m Map) iterFromRange(ctx context.Context, rng Range) (*orderedTreeIter[val.Tuple, val.Tuple], error) {
	return treeIterFromRange(ctx, m.tuples.root, m.tuples.ns, rng)
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

	startSearch := rangeStartSearchFn(rng)
	if rng.Start == nil {
		start, err = tree.NewCursorAtStart(ctx, ns, root)
	} else {
		start, err = tree.NewCursorFromSearchFn(ctx, ns, root, startSearch)
	}
	if err != nil {
		return nil, err
	}

	stopSearch := rangeStopSearchFn(rng)
	if rng.Stop == nil {
		stop, err = tree.NewCursorPastEnd(ctx, ns, root)
	} else {
		stop, err = tree.NewCursorFromSearchFn(ctx, ns, root, stopSearch)
	}
	if err != nil {
		return nil, err
	}

	if start.Compare(stop) >= 0 {
		start = nil // empty range
	}

	return &orderedTreeIter[val.Tuple, val.Tuple]{curr: start, stop: stop}, nil
}

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

func newEmptyMapNode(pool pool.BuffPool) tree.Node {
	bld := &mapBuilder{}
	return bld.Build(pool)
}

func newMapBuilder(level int) *mapBuilder {
	return &mapBuilder{level: level}
}

var _ tree.NodeBuilderFactory[*mapBuilder] = newMapBuilder

type mapBuilder struct {
	keys, values []tree.Item
	size, level  int

	subtrees tree.SubtreeCounts
}

var _ tree.NodeBuilder = &mapBuilder{}

func (nb *mapBuilder) StartNode() {
	nb.reset()
}

func (nb *mapBuilder) HasCapacity(key, value tree.Item) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(tree.MaxVectorOffset)
}

func (nb *mapBuilder) AddItems(key, value tree.Item, subtree uint64) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *mapBuilder) Count() int {
	return len(nb.keys)
}

func (nb *mapBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	nb.keys = nb.keys[:0]
	nb.values = nb.values[:0]
	nb.size = 0
	nb.subtrees = nb.subtrees[:0]
}

func (nb *mapBuilder) Build(pool pool.BuffPool) (node tree.Node) {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := estimateBufferSize(nb.keys, nb.values, nb.subtrees)
	b := getFlatbufferBuilder(pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, nb.keys, keySz)
	serial.ProllyTreeNodeStartKeyOffsetsVector(b, len(nb.keys)-1)
	keyOffs = writeItemOffsets(b, nb.keys, keySz)

	if nb.level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, nb.values, valSz)
		serial.ProllyTreeNodeStartValueOffsetsVector(b, len(nb.values)-1)
		valOffs = writeItemOffsets(b, nb.values, valSz)
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, nb.values, valSz)
		cardArr = writeCountArray(b, nb.subtrees)
	}

	// populate the node's vtable
	serial.ProllyTreeNodeStart(b)
	serial.ProllyTreeNodeAddKeyItems(b, keyTups)
	serial.ProllyTreeNodeAddKeyOffsets(b, keyOffs)
	if nb.level == 0 {
		serial.ProllyTreeNodeAddValueItems(b, valTups)
		serial.ProllyTreeNodeAddValueOffsets(b, valOffs)
		serial.ProllyTreeNodeAddTreeCount(b, uint64(len(nb.keys)))
	} else {
		serial.ProllyTreeNodeAddAddressArray(b, refArr)
		serial.ProllyTreeNodeAddSubtreeCounts(b, cardArr)
		serial.ProllyTreeNodeAddTreeCount(b, nb.subtrees.Sum())
	}
	serial.ProllyTreeNodeAddKeyType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddValueType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddTreeLevel(b, uint8(nb.level))
	b.Finish(serial.ProllyTreeNodeEnd(b))
	nb.reset()

	buf := b.FinishedBytes()
	return tree.NodeFromBytes(buf)
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
