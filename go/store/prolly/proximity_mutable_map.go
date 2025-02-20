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

package prolly

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
)

type ProximityMutableMap = GenericMutableMap[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]]

type ProximityFlusher struct {
	logChunkSize uint8
	distanceType vector.DistanceType
}

var _ MutableMapFlusher[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]] = ProximityFlusher{}

func (f ProximityFlusher) ApplyMutationsWithSerializer(
	ctx context.Context,
	serializer message.Serializer,
	mutableMap *GenericMutableMap[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]],
) (tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc], error) {
	// Identify what parts of the tree need to be rebuilt:
	// For each edit, identify the node closest to the root that is affected.
	// Then, walk the tree creating a new one.
	// In order to skip walking parts of the tree that aren't modified, we need to know when a node
	// has no edits in any of its children.
	// We can have a cursor that fast-forwards to the affected value.
	// - How does this work with inserts?
	// Do it recursively, starting with the root. Sort each edit into the affected child node (or the current node).
	// If the current node if affected, rebuild.
	// Otherwise visit each child node.
	keyDesc := mutableMap.keyDesc
	valDesc := mutableMap.valDesc
	ns := mutableMap.NodeStore()
	convert := func(ctx context.Context, bytes []byte) []float64 {
		h, _ := keyDesc.GetJSONAddr(0, bytes)
		doc := tree.NewJSONDoc(h, ns)
		jsonWrapper, err := doc.ToIndexedJSONDocument(ctx)
		if err != nil {
			panic(err)
		}
		floats, err := sql.ConvertToVector(jsonWrapper)
		if err != nil {
			panic(err)
		}
		return floats
	}
	edits := make([]VectorIndexKV, 0, mutableMap.tuples.Edits.Count())
	editIter := mutableMap.tuples.Mutations()
	key, value := editIter.NextMutation(ctx)
	maxEditLevel := uint8(0)
	for key != nil {
		keyLevel := tree.DeterministicHashLevel(f.logChunkSize, key)
		if keyLevel > maxEditLevel {
			maxEditLevel = keyLevel
		}
		edits = append(edits, VectorIndexKV{
			key:   key,
			value: value,
			level: int(keyLevel),
		})
		key, value = editIter.NextMutation(ctx)
	}
	var newRoot tree.Node
	var err error
	root := mutableMap.tuples.Static.Root
	distanceType := mutableMap.tuples.Static.DistanceType
	if root.Count() == 0 {
		// Original index was empty. We need to make a new index based on the edits.
		newRoot, err = makeNewProximityMap(ctx, ns, edits, distanceType, keyDesc, valDesc, f.logChunkSize)
	} else if maxEditLevel >= uint8(root.Level()) {
		// The root node has changed, or there may be a new level to the tree. We need to rebuild the tree.
		newRoot, _, err = f.rebuildNode(ctx, ns, root, edits, distanceType, keyDesc, valDesc, maxEditLevel)
	} else {
		newRoot, _, err = f.visitNode(ctx, serializer, ns, root, edits, convert, distanceType, keyDesc, valDesc)

	}
	if err != nil {
		return tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]{}, err
	}
	return tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:         newRoot,
		NodeStore:    ns,
		DistanceType: distanceType,
		Convert:      convert,
		Order:        keyDesc,
	}, nil
}

type VectorIndexKV struct {
	key, value tree.Item
	level      int
}

type childEditList struct {
	edits       []VectorIndexKV
	mustRebuild bool
}

func makeNewProximityMap(
	ctx context.Context,
	ns tree.NodeStore,
	edits []VectorIndexKV,
	distanceType vector.DistanceType,
	keyDesc val.TupleDesc,
	valDesc val.TupleDesc,
	logChunkSize uint8,
) (newNode tree.Node, err error) {
	proximityMapBuilder, err := NewProximityMapBuilder(ctx, ns, distanceType, keyDesc, valDesc, logChunkSize)
	if err != nil {
		return tree.Node{}, err
	}
	for _, edit := range edits {
		// If the original index was empty, then all edits are inserts.
		if edit.key != nil {
			err = proximityMapBuilder.InsertAtLevel(ctx, edit.key, edit.value, uint8(edit.level))
			if err != nil {
				return tree.Node{}, err
			}
		}
	}
	proximityMap, err := proximityMapBuilder.Flush(ctx)
	if err != nil {
		return tree.Node{}, err
	}

	return proximityMap.Node(), nil
}

// visitNode produces a new tree.Node that incorporates the provided edits to the provided node.
// As a precondition, we have confirmed that the keys in the provided node will not change, but the
// keys in children nodes might. If the keys in a child node would change, we call rebuildNode on that child.
// Otherwise, we recursively called visitNode on the children.
func (f ProximityFlusher) visitNode(
	ctx context.Context,
	serializer message.Serializer,
	ns tree.NodeStore,
	node tree.Node,
	edits []VectorIndexKV,
	convert func(context.Context, []byte) []float64,
	distanceType vector.DistanceType,
	keyDesc val.TupleDesc,
	valDesc val.TupleDesc,
) (newNode tree.Node, subtrees int, err error) {
	var keys [][]byte
	var values [][]byte
	var nodeSubtrees []uint64

	if node.IsLeaf() {
		keys, values, nodeSubtrees = f.rebuildLeafNodeWithEdits(ctx, node, edits, keyDesc)
	} else {
		// sort the list of edits based on which child node contains them.
		childEdits := make(map[int]childEditList)
		for _, edit := range edits {
			key := edit.key
			editVector := convert(ctx, key)
			level := edit.level
			// visit each child in the node to determine which is closest
			closestIdx := 0
			childKey := node.GetKey(0)
			closestDistance, err := distanceType.Eval(convert(ctx, childKey), editVector)
			if err != nil {
				return tree.Node{}, 0, err
			}
			for i := 1; i < node.Count(); i++ {
				childKey = node.GetKey(i)
				newDistance, err := distanceType.Eval(convert(ctx, childKey), editVector)
				if err != nil {
					return tree.Node{}, 0, err
				}
				if newDistance < closestDistance {
					closestDistance = newDistance
					closestIdx = i
				}
			}
			childEditList := childEdits[closestIdx]
			childEditList.edits = append(childEditList.edits, edit)
			if level == node.Level()-1 {
				childEditList.mustRebuild = true
			}
			childEdits[closestIdx] = childEditList
		}
		// Recursively build the new tree.
		// We need keys, values, subtrees, and levels.
		for i := 0; i < node.Count(); i++ {
			childKey := node.GetKey(i)
			keys = append(keys, childKey)
			childValue := node.GetValue(i)

			childEditList := childEdits[i]
			if len(childEditList.edits) == 0 {
				// No edits affected this node, leave it as is.
				values = append(values, childValue)
			} else {
				childNodeAddress := hash.New(childValue)
				childNode, err := ns.Read(ctx, childNodeAddress)
				if err != nil {
					return tree.Node{}, 0, err
				}
				var newChildNode tree.Node
				var childSubtrees int
				if childEditList.mustRebuild {
					newChildNode, childSubtrees, err = f.rebuildNode(ctx, ns, childNode, childEditList.edits, distanceType, keyDesc, valDesc, uint8(childNode.Level()))
				} else {
					newChildNode, childSubtrees, err = f.visitNode(ctx, serializer, ns, childNode, childEditList.edits, convert, distanceType, keyDesc, valDesc)
				}

				if err != nil {
					return tree.Node{}, 0, err
				}
				newChildAddress := newChildNode.HashOf()

				values = append(values, newChildAddress[:])
				nodeSubtrees = append(nodeSubtrees, uint64(childSubtrees))
			}
		}
	}
	newNode, err = serializeVectorIndexNode(ctx, serializer, ns, keys, values, nodeSubtrees, node.Level())
	if err != nil {
		return tree.Node{}, 0, err
	}
	subtrees, err = newNode.TreeCount()
	if err != nil {
		return tree.Node{}, 0, err
	}
	return newNode, subtrees, err
}

func serializeVectorIndexNode(
	ctx context.Context,
	serializer message.Serializer,
	ns tree.NodeStore,
	keys [][]byte,
	values [][]byte,
	nodeSubtrees []uint64,
	level int,
) (tree.Node, error) {
	msg := serializer.Serialize(keys, values, nodeSubtrees, level)
	newNode, fileId, err := tree.NodeFromBytes(msg)
	if err != nil {
		return tree.Node{}, err
	}

	if fileId != serial.VectorIndexNodeFileID {
		return tree.Node{}, fmt.Errorf("expected file id %s, received %s", serial.VectorIndexNodeFileID, fileId)
	}
	_, err = ns.Write(ctx, newNode)
	return newNode, err
}

// rebuildLeafNodeWithEdits creates a new leaf node by applying a list of edits to an existing node.
func (f ProximityFlusher) rebuildLeafNodeWithEdits(
	ctx context.Context,
	originalNode tree.Node,
	edits []VectorIndexKV,
	keyDesc val.TupleDesc,
) (keys [][]byte, values [][]byte, nodeSubtrees []uint64) {
	// combine edits with node keys. Use merge sort.

	editIdx := 0
	nodeIdx := 0
	for editIdx < len(edits) || nodeIdx < originalNode.Count() {
		// Edit doesn't match an existing key: it must be an insert.
		if editIdx >= len(edits) {
			keys = append(keys, originalNode.GetKey(nodeIdx))
			values = append(values, originalNode.GetValue(nodeIdx))
			nodeSubtrees = append(nodeSubtrees, 0)
			nodeIdx++
			continue
		}
		if nodeIdx >= originalNode.Count() {
			keys = append(keys, edits[editIdx].key)
			values = append(values, edits[editIdx].value)
			nodeSubtrees = append(nodeSubtrees, 0)
			editIdx++
			continue
		}
		editKey := val.Tuple(edits[editIdx].key)
		nodeKey := val.Tuple(originalNode.GetKey(nodeIdx))
		cmp := keyDesc.Compare(ctx, editKey, nodeKey)
		if cmp < 0 {
			//edit comes first
			// Edit doesn't match an existing key: it must be an insert.
			keys = append(keys, edits[editIdx].key)
			values = append(values, edits[editIdx].value)
			nodeSubtrees = append(nodeSubtrees, 0)
			editIdx++
			continue
		}
		if cmp > 0 {
			// node comes first
			keys = append(keys, originalNode.GetKey(nodeIdx))
			values = append(values, originalNode.GetValue(nodeIdx))
			nodeSubtrees = append(nodeSubtrees, 0)
			nodeIdx++
			continue
		}
		// edit to an existing key.
		newValue := edits[editIdx].value
		editIdx++
		nodeIdx++
		if newValue == nil {
			// This is a delete. We simply skip to the next key, excluding this key from the new node.
			continue
		}
		keys = append(keys, editKey)
		values = append(values, newValue)
		nodeSubtrees = append(nodeSubtrees, 0)
	}
	return
}

var DefaultLogChunkSize = uint8(8)

func (f ProximityFlusher) rebuildNode(ctx context.Context, ns tree.NodeStore, node tree.Node, edits []VectorIndexKV, distanceType vector.DistanceType, keyDesc val.TupleDesc, valDesc val.TupleDesc, maxLevel uint8) (newNode tree.Node, subtrees int, err error) {

	proximityMapBuilder, err := NewProximityMapBuilder(ctx, ns, distanceType, keyDesc, valDesc, f.logChunkSize)
	if err != nil {
		return tree.Node{}, 0, err
	}
	editSkipList := skip.NewSkipList(func(ctx context.Context, left, right []byte) int {
		return keyDesc.Compare(ctx, left, right)
	})
	for _, edit := range edits {
		editSkipList.Put(ctx, edit.key, edit.value)
	}

	insertFromNode := func(nd tree.Node, i int) error {
		key := nd.GetKey(i)
		value := nd.GetValue(i)
		_, hasNewVal := editSkipList.Get(ctx, key)
		if !hasNewVal {
			// TODO: Is it faster if we fetch the level from the current tree?
			keyLevel := tree.DeterministicHashLevel(f.logChunkSize, key)
			if keyLevel > maxLevel {
				keyLevel = maxLevel
			}
			err = proximityMapBuilder.InsertAtLevel(ctx, key, value, keyLevel)
			if err != nil {
				return err
			}
		}
		return nil
	}

	var walk func(nd tree.Node) error
	walk = func(nd tree.Node) (err error) {

		if nd.IsLeaf() {
			for i := 0; i < nd.Count(); i++ {
				err = insertFromNode(nd, i)
				if err != nil {
					return err
				}
			}
		} else {

			for i := 0; i < nd.Count(); i++ {
				childAddr := hash.New(nd.GetValue(i))
				if i != 0 {
					// walkLevel = nd.Level()
				}
				child, err := ns.Read(ctx, childAddr)
				if err != nil {
					return err
				}
				err = walk(child)
			}
		}

		return nil
	}

	err = walk(node)
	if err != nil {
		return tree.Node{}, 0, err
	}
	for _, edit := range edits {
		key := edit.key
		value := edit.value
		if value != nil {
			err = proximityMapBuilder.Insert(ctx, key, value)
			if err != nil {
				return tree.Node{}, 0, err
			}
		}
	}
	newMap, err := proximityMapBuilder.Flush(ctx)
	if err != nil {
		return tree.Node{}, 0, err
	}
	newRoot := newMap.tuples.Root
	newTreeCount, err := newRoot.TreeCount()
	if err != nil {
		return tree.Node{}, 0, err
	}
	return newRoot, newTreeCount, nil
}

func (f ProximityFlusher) GetDefaultSerializer(ctx context.Context, mutableMap *GenericMutableMap[ProximityMap, tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]]) message.Serializer {
	return message.NewVectorIndexSerializer(mutableMap.NodeStore().Pool(), f.logChunkSize, f.distanceType)
}

// newMutableMap returns a new MutableMap.
func newProximityMutableMap(m ProximityMap) *ProximityMutableMap {
	return &ProximityMutableMap{
		tuples:     m.tuples.Mutate(),
		keyDesc:    m.keyDesc,
		valDesc:    m.valDesc,
		maxPending: defaultMaxPending,
		flusher:    ProximityFlusher{logChunkSize: m.logChunkSize, distanceType: m.tuples.DistanceType},
	}
}

func (f ProximityFlusher) MapInterface(ctx context.Context, mut *ProximityMutableMap) (MapInterface, error) {
	return f.Map(ctx, mut)
}

// TreeMap materializes all pending and applied mutations in the MutableMap.
func (f ProximityFlusher) TreeMap(ctx context.Context, mut *ProximityMutableMap) (tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc], error) {
	s := message.NewVectorIndexSerializer(mut.NodeStore().Pool(), f.logChunkSize, f.distanceType)
	return mut.flushWithSerializer(ctx, s)
}

// TreeMap materializes all pending and applied mutations in the MutableMap.
func (f ProximityFlusher) Map(ctx context.Context, mut *ProximityMutableMap) (ProximityMap, error) {
	treeMap, err := f.TreeMap(ctx, mut)
	if err != nil {
		return ProximityMap{}, err
	}
	return ProximityMap{
		tuples:       treeMap,
		keyDesc:      mut.keyDesc,
		valDesc:      mut.valDesc,
		logChunkSize: f.logChunkSize,
	}, nil
}
