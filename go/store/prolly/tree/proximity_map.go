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

package tree

import (
	"container/heap"
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"math"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/esote/minmaxheap"
)

type KeyValueDistanceFn[K, V ~[]byte] func(key K, value V, distance float64) error

// ProximityMap is a static Prolly Tree where the position of a key in the tree is based on proximity, as opposed to a traditional ordering.
// O provides the ordering only within a node.
type ProximityMap[K, V ~[]byte, O Ordering[K]] struct {
	Root         Node
	NodeStore    NodeStore
	DistanceType vector.DistanceType
	Convert      func([]byte) []float64
	Order        O
}

func (t ProximityMap[K, V, O]) GetRoot() Node {
	return t.Root
}

func (t ProximityMap[K, V, O]) GetNodeStore() NodeStore {
	return t.NodeStore
}

func (t ProximityMap[K, V, O]) GetPrefix(ctx context.Context, query K, prefixOrder O, cb KeyValueFn[K, V]) (err error) {
	//TODO implement me
	panic("implement me")
}

func (t ProximityMap[K, V, O]) HasPrefix(ctx context.Context, query K, prefixOrder O) (ok bool, err error) {
	//TODO implement me
	panic("implement me")
}

func (t ProximityMap[K, V, O]) Mutate() MutableMap[K, V, O, ProximityMap[K, V, O]] {
	return MutableMap[K, V, O, ProximityMap[K, V, O]]{
		Edits: skip.NewSkipList(func(left, right []byte) int {
			return t.Order.Compare(left, right)
		}),
		Static: t,
	}
}

func (t ProximityMap[K, V, O]) IterKeyRange(ctx context.Context, start, stop K) (*OrderedTreeIter[K, V], error) {
	panic("Not implemented")
}

func (t ProximityMap[K, V, O]) Count() (int, error) {
	return t.Root.TreeCount()
}

func (t ProximityMap[K, V, O]) Height() int {
	return t.Root.Level() + 1
}

func (t ProximityMap[K, V, O]) HashOf() hash.Hash {
	return t.Root.HashOf()
}

func (t ProximityMap[K, V, O]) WalkAddresses(ctx context.Context, cb AddressCb) error {
	return WalkAddresses(ctx, t.Root, t.NodeStore, cb)
}

func (t ProximityMap[K, V, O]) WalkNodes(ctx context.Context, cb NodeCb) error {
	return WalkNodes(ctx, t.Root, t.NodeStore, cb)
}

// Get searches for an exact vector in the index, calling |cb| with the matching key-value pairs.
func (t ProximityMap[K, V, O]) Get(ctx context.Context, query K, cb KeyValueFn[K, V]) (err error) {
	nd := t.Root

	queryVector := t.Convert(query)

	// Find the child with the minimum distance.

	for {
		var closestKey K
		var closestIdx int
		distance := math.Inf(1)

		for i := 0; i < int(nd.count); i++ {
			k := nd.GetKey(i)
			newDistance, err := t.DistanceType.Eval(t.Convert(k), queryVector)
			if err != nil {
				return err
			}
			if newDistance < distance {
				closestIdx = i
				distance = newDistance
				closestKey = []byte(k)
			}
		}

		if nd.IsLeaf() {
			return cb(closestKey, []byte(nd.GetValue(closestIdx)))
		}

		nd, err = fetchChild(ctx, t.NodeStore, nd.getAddress(closestIdx))
		if err != nil {
			return err
		}
	}
}

func (t ProximityMap[K, V, O]) Has(ctx context.Context, query K) (ok bool, err error) {
	err = t.Get(ctx, query, func(_ K, _ V) error {
		ok = true
		return nil
	})
	return ok, err
}

type DistancePriorityHeapElem struct {
	key      Item
	value    Item
	distance float64
}

type DistancePriorityHeap []DistancePriorityHeapElem

var _ heap.Interface = (*DistancePriorityHeap)(nil)

func newNodePriorityHeap(capacity int) DistancePriorityHeap {
	// Allocate one extra slot: whenever this fills we remove the max element.
	return make(DistancePriorityHeap, 0, capacity+1)
}

func (n DistancePriorityHeap) Len() int {
	return len(n)
}

func (n DistancePriorityHeap) Less(i, j int) bool {
	return n[i].distance < n[j].distance
}

func (n DistancePriorityHeap) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n *DistancePriorityHeap) Push(x any) {
	*n = append(*n, x.(DistancePriorityHeapElem))
}

func (n *DistancePriorityHeap) Pop() any {
	length := len(*n)
	last := (*n)[length-1]
	*n = (*n)[:length-1]
	return last
}

func (n *DistancePriorityHeap) Insert(key Item, value Item, distance float64) {
	minmaxheap.Push(n, DistancePriorityHeapElem{
		key:      key,
		value:    value,
		distance: distance,
	})
	if len(*n) == cap(*n) {
		minmaxheap.PopMax(n)
	}
}

// GetClosest performs an approximate nearest neighbors search. It finds |limit| vectors that are close to the query vector,
// and calls |cb| with the matching key-value pairs.
func (t ProximityMap[K, V, O]) GetClosest(ctx context.Context, query interface{}, cb KeyValueDistanceFn[K, V], limit int) (err error) {
	if limit == 0 {
		return nil
	}

	queryVector, err := sql.ConvertToVector(query)
	if err != nil {
		return err
	}

	// |nodes| holds the current candidates for closest vectors, up to |limit|
	nodes := newNodePriorityHeap(limit)

	for i := 0; i < int(t.Root.count); i++ {
		k := t.Root.GetKey(i)
		newDistance, err := t.DistanceType.Eval(t.Convert(k), queryVector)
		if err != nil {
			return err
		}
		nodes.Insert(k, t.Root.GetValue(i), newDistance)
	}

	for level := t.Root.Level() - 1; level >= 0; level-- {
		// visit each candidate node at the current level, building a priority list of candidates for the next level.
		nextLevelNodes := newNodePriorityHeap(limit)

		for _, keyAndDistance := range nodes {
			address := keyAndDistance.value

			node, err := fetchChild(ctx, t.NodeStore, hash.New(address))
			if err != nil {
				return err
			}
			// TODO: We don't need to recompute the distance when visiting the same key as the parent.
			for i := 0; i < int(node.count); i++ {
				k := node.GetKey(i)
				newDistance, err := t.DistanceType.Eval(t.Convert(k), queryVector)
				if err != nil {
					return err
				}
				nextLevelNodes.Insert(k, node.GetValue(i), newDistance)
			}
		}
		nodes = nextLevelNodes
	}

	for nodes.Len() > 0 {
		node := minmaxheap.Pop(&nodes).(DistancePriorityHeapElem)
		err := cb([]byte(node.key), []byte(node.value), node.distance)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t ProximityMap[K, V, O]) IterAll(ctx context.Context) (*OrderedTreeIter[K, V], error) {
	c, err := newCursorAtStart(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	s, err := newCursorPastEnd(ctx, t.NodeStore, t.Root)
	if err != nil {
		return nil, err
	}

	stop := func(curr *cursor) bool {
		return curr.compare(s) >= 0
	}

	if stop(c) {
		// empty range
		return &OrderedTreeIter[K, V]{curr: nil}, nil
	}

	return &OrderedTreeIter[K, V]{curr: c, stop: stop, step: c.advance}, nil
}

func getJsonValueFromHash(ctx context.Context, ns NodeStore, h hash.Hash) (interface{}, error) {
	return NewJSONDoc(h, ns).ToIndexedJSONDocument(ctx)
}

func getVectorFromHash(ctx context.Context, ns NodeStore, h hash.Hash) ([]float64, error) {
	otherValue, err := getJsonValueFromHash(ctx, ns, h)
	if err != nil {
		return nil, err
	}
	return sql.ConvertToVector(otherValue)
}
