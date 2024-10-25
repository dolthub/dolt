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
	"context"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"math"

	"github.com/dolthub/dolt/go/store/hash"
)

type KeyValueDistanceFn[K, V ~[]byte] func(key K, value V, distance float64) error

// ProximityMap is a static Prolly Tree where the position of a key in the tree is based on proximity, as opposed to a traditional ordering.
// O provides the ordering only within a node.
type ProximityMap[K, V ~[]byte, O Ordering[K]] struct {
	Root         Node
	NodeStore    NodeStore
	DistanceType expression.DistanceType
	Convert      func([]byte) []float64
	Order        O
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

// GetExact searches for an exact vector in the index, calling |cb| with the matching key-value pairs.
func (t ProximityMap[K, V, O]) GetExact(ctx context.Context, query interface{}, cb KeyValueFn[K, V]) (err error) {
	nd := t.Root

	queryVector, err := sql.ConvertToVector(query)
	if err != nil {
		return err
	}

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
	err = t.GetExact(ctx, query, func(_ K, _ V) error {
		ok = true
		return nil
	})
	return ok, err
}

// GetClosest performs an approximate nearest neighbors search. It finds |limit| vectors that are close to the query vector,
// and calls |cb| with the matching key-value pairs.
func (t ProximityMap[K, V, O]) GetClosest(ctx context.Context, query interface{}, cb KeyValueDistanceFn[K, V], limit int) (err error) {
	if limit != 1 {
		return fmt.Errorf("currently only limit = 1 (find single closest vector) is supported for ProximityMap")
	}

	queryVector, err := sql.ConvertToVector(query)
	if err != nil {
		return err
	}

	nd := t.Root

	var closestKey K
	var closestIdx int
	distance := math.Inf(1)

	for {
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
			return cb(closestKey, []byte(nd.GetValue(closestIdx)), distance)
		}

		nd, err = fetchChild(ctx, t.NodeStore, nd.getAddress(closestIdx))
		if err != nil {
			return err
		}
	}
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
