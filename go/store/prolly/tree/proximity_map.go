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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
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

// Building/inserting into a ProximityMap requires a Fixup step, which reorganizes part (in the worst case, all) of the
// tree such that each node is a child of the closest node in the previous row.
// Currently, this is a brute force approach that visits the entire affected region of the tree in level-order, builds
// a new tree structure in memory, and then serializes the new tree to disk. There is room to improvement here.

// An in-memory representation of a Vector Index node.
// It stores a list of (vectorHash, key, value OR address) tuples.
// The first element is always the same vector used as the parent key.
// The remaining elements are sorted prior to serialization.
type memoryNode struct {
	vectorHashes []hash.Hash
	keys         [][]byte
	addresses    []memoryNode
	values       [][]byte
}

type memoryNodeSort[K ~[]byte, O Ordering[K]] struct {
	*memoryNode
	order  O
	isRoot bool
}

var _ sort.Interface = (*memoryNodeSort[[]byte, Ordering[[]byte]])(nil)

func (m memoryNodeSort[K, O]) Len() int {
	keys := m.keys[1:]
	if m.isRoot {
		keys = m.keys
	}
	return len(keys)
}

func (m memoryNodeSort[K, O]) Less(i, j int) bool {
	keys := m.keys[1:]
	if m.isRoot {
		keys = m.keys
	}
	return m.order.Compare(keys[i], keys[j]) < 0
}

func (m memoryNodeSort[K, O]) Swap(i, j int) {

	vectorHashes := m.vectorHashes[1:]
	if m.isRoot {
		vectorHashes = m.vectorHashes
	}
	vectorHashes[i], vectorHashes[j] = vectorHashes[j], vectorHashes[i]
	keys := m.keys[1:]
	if m.isRoot {
		keys = m.keys
	}
	keys[i], keys[j] = keys[j], keys[i]
	if m.addresses != nil {
		addresses := m.addresses[1:]
		if m.isRoot {
			addresses = m.addresses
		}
		addresses[i], addresses[j] = addresses[j], addresses[i]
	}
	if m.values != nil {
		values := m.values[1:]
		if m.isRoot {
			values = m.values
		}
		values[i], values[j] = values[j], values[i]
	}
}

func serializeAndWriteNode(ctx context.Context, ns NodeStore, s message.Serializer, level int, subtrees []uint64, keys [][]byte, values [][]byte) (node Node, err error) {
	msg := s.Serialize(keys, values, subtrees, level)
	node, err = NodeFromBytes(msg)
	if err != nil {
		return Node{}, err
	}
	_, err = ns.Write(ctx, node)
	return node, err
}

func serializeMemoryNode[K ~[]byte, O Ordering[K]](ctx context.Context, m memoryNode, ns NodeStore, s message.Serializer, level int, isRoot bool, order O) (node Node, err error) {
	sort.Sort(memoryNodeSort[K, O]{
		memoryNode: &m,
		isRoot:     isRoot,
		order:      order,
	})
	if level == 0 {
		return serializeAndWriteNode(ctx, ns, s, 0, nil, m.keys, m.values)
	}
	values := make([][]byte, 0, len(m.addresses))
	subTrees := make([]uint64, 0, len(m.addresses))
	for _, address := range m.addresses {
		child, err := serializeMemoryNode(ctx, address, ns, s, level-1, false, order)
		if err != nil {
			return Node{}, err
		}
		childHash := child.HashOf()
		values = append(values, childHash[:])
		childCount, err := message.GetTreeCount(child.msg)
		if err != nil {
			return Node{}, err
		}
		subTrees = append(subTrees, uint64(childCount))
	}
	return serializeAndWriteNode(ctx, ns, s, level, subTrees, m.keys, values)
}

func (m *memoryNode) insert(ctx context.Context, ns NodeStore, distanceType expression.DistanceType, vectorHash hash.Hash, key Item, value Item, vector []float64, level int, isLeaf bool) error {
	if level == 0 {
		if isLeaf {
			if bytes.Equal(m.keys[0], key) {
				m.values[0] = value
			} else {
				m.vectorHashes = append(m.vectorHashes, vectorHash)
				m.keys = append(m.keys, key)
				m.values = append(m.values, value)
			}
			return nil
		}
		// We're inserting into the row that's currently the bottom of the in-memory representation,
		// but this isn't the leaf row of the final tree: more rows will be added afterward.
		if bytes.Equal(m.keys[0], key) {
			m.addresses[0] = memoryNode{
				vectorHashes: []hash.Hash{vectorHash},
				keys:         [][]byte{key},
				addresses:    []memoryNode{{}},
				values:       [][]byte{nil},
			}
		} else {
			m.vectorHashes = append(m.vectorHashes, vectorHash)
			m.keys = append(m.keys, key)
			m.addresses = append(m.addresses, memoryNode{
				vectorHashes: []hash.Hash{vectorHash},
				keys:         [][]byte{key},
				addresses:    []memoryNode{{}},
				values:       [][]byte{nil},
			})
		}
		return nil
	}
	closestIdx := 0
	otherVector, err := getVectorFromHash(ctx, ns, m.vectorHashes[0])
	if err != nil {
		return err
	}
	distance, err := distanceType.Eval(vector, otherVector)
	if err != nil {
		return err
	}
	for i := 1; i < len(m.keys); i++ {
		candidateVector, err := getVectorFromHash(ctx, ns, m.vectorHashes[i])
		if err != nil {
			return err
		}
		candidateDistance, err := distanceType.Eval(vector, candidateVector)
		if err != nil {
			return err
		}
		if candidateDistance < distance {
			distance = candidateDistance
			closestIdx = i
		}
	}
	return m.addresses[closestIdx].insert(ctx, ns, distanceType, vectorHash, key, value, vector, level-1, isLeaf)
}

func levelTraversal(ctx context.Context, nd Node, ns NodeStore, level int, cb func(nd Node) error) error {
	if level == 0 {
		return cb(nd)
	}
	for i := 0; i < int(nd.count); i++ {
		child, err := ns.Read(ctx, nd.getAddress(i))
		if err != nil {
			return err
		}
		err = levelTraversal(ctx, child, ns, level-1, cb)
		if err != nil {
			return err
		}
	}
	return nil
}

// FixupProximityMap takes the root not of a vector index which may not be in the correct order, and moves and reorders
// nodes to make it correct. It ensures the following invariants:
//   - In any node except the root node, the first key is the same as the key in the edge pointing to that node.
//     (This is the node's "defining key")
//   - All other keys within a node are sorted.
//   - Each non-root node contains only the keys (including transitively) that are closer to that node's defining key than
//     any other key in that node's parent.
func FixupProximityMap[K ~[]byte, O Ordering[K]](ctx context.Context, ns NodeStore, distanceType expression.DistanceType, n Node, getHash func([]byte) hash.Hash, order O) (Node, error) {
	if n.Level() == 0 {
		return n, nil
	}
	// Iterate over the keys, starting at the level 1 nodes (with root as level 0)
	result := memoryNode{
		vectorHashes: make([]hash.Hash, n.Count()),
		keys:         make([][]byte, n.Count()),
		addresses:    make([]memoryNode, n.Count()),
	}
	for i := 0; i < n.Count(); i++ {
		keyItem := n.GetKey(i)
		result.keys[i] = keyItem
		vectorHash := getHash(keyItem)
		result.vectorHashes[i] = vectorHash
		result.addresses[i] = memoryNode{
			vectorHashes: []hash.Hash{vectorHash},
			keys:         [][]byte{keyItem},
			addresses:    []memoryNode{{}},
			values:       [][]byte{nil},
		}
	}

	for level := 1; level <= n.Level(); level++ {
		// Insert each key into the appropriate place in the result.
		err := levelTraversal(ctx, n, ns, level, func(nd Node) error {
			for i := 0; i < nd.Count(); i++ {
				key := nd.GetKey(i)
				vecHash := getHash(key)
				vector, err := getVectorFromHash(ctx, ns, vecHash)
				if err != nil {
					return err
				}
				isLeaf := level == n.Level()
				err = result.insert(ctx, ns, distanceType, vecHash, key, nd.GetValue(i), vector, level, isLeaf)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return Node{}, err
		}
	}
	// Convert the in-memory representation back into a Node.
	serializer := message.NewVectorIndexSerializer(ns.Pool())
	return serializeMemoryNode[K, O](ctx, result, ns, serializer, n.Level(), true, order)
}
