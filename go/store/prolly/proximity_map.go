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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"io"
)

// ProximityMap wraps a tree.ProximityMap but operates on typed Tuples instead of raw bytestrings.
type ProximityMap struct {
	tuples  tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

// NewProximityMap creates a new ProximityMap from a supplied root node.
func NewProximityMap(ctx context.Context, ns tree.NodeStore, node tree.Node, keyDesc val.TupleDesc, valDesc val.TupleDesc, distanceType expression.DistanceType) ProximityMap {
	tuples := tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:         node,
		NodeStore:    ns,
		Order:        keyDesc,
		DistanceType: distanceType,
		Convert: func(bytes []byte) []float64 {
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
		},
	}
	return ProximityMap{
		tuples:  tuples,
		keyDesc: keyDesc,
		valDesc: valDesc,
	}
}

func getJsonValueFromHash(ctx context.Context, ns tree.NodeStore, h hash.Hash) (interface{}, error) {
	return tree.NewJSONDoc(h, ns).ToIndexedJSONDocument(ctx)
}

func getVectorFromHash(ctx context.Context, ns tree.NodeStore, h hash.Hash) ([]float64, error) {
	otherValue, err := getJsonValueFromHash(ctx, ns, h)
	if err != nil {
		return nil, err
	}
	return sql.ConvertToVector(otherValue)
}

func NewProximityMapFromTupleIter(ctx context.Context, ns tree.NodeStore, distanceType expression.DistanceType, keyDesc val.TupleDesc, valDesc val.TupleDesc, keys [][]byte, values [][]byte, logChunkSize uint8) (ProximityMap, error) {
	// The algorithm for building a ProximityMap's tree requires us to start at the root and build out to the leaf nodes.
	// Given that our trees are Merkle Trees, this presents an obvious problem.
	// Our solution is to create the final tree by applying a series of transformations to intermediate trees.

	// Note: when talking about tree levels, we use "level" when counting from the leaves, and "depth" when counting
	// from the root. In a tree with 5 levels, the root is level 4 (and depth 0), while the leaves are level 0 (and depth 4)

	// The process looks like this:
	// Step 1: Create `levelMap`, a map from (indexLevel, keyBytes) -> values
	//   - indexLevel: the minimum level in which the vector appears
	//   - keyBytes: a bytestring containing the bytes of the ProximityMap key (which includes the vector)
	//   - values: the ProximityMap value tuple
	//
	// Step 2: Create `pathMaps`, a list of maps, each corresponding to a different level of the ProximityMap
	//   The pathMap at depth `i` has the schema (vectorAddrs[1]...vectorAddr[i], keyBytes) -> value
	//   and contains a row for every vector whose maximum depth is i.
	//   - vectorAddrs: the path of vectors visited when walking from the root to the maximum depth where the vector appears.
	//   - keyBytes: a bytestring containing the bytes of the ProximityMap key (which includes the vector)
	//   - values: the ProximityMap value tuple
	//
	//   These maps must be built in order, from shallowest to deepest.
	//
	// Step 3: Create an iter over each `pathMap` created in the previous step, and walk the shape of the final ProximityMap,
	// generating Nodes as we go.
	//
	// Currently, the intermediate trees are created using the standard NodeStore. This means that the nodes of these
	// trees will inevitably be written out to disk when the NodeStore flushes, despite the fact that we know they
	// won't be needed once we finish building the ProximityMap. This could potentially be avoided by creating a
	// separate in-memory NodeStore for these values.

	vectorIndexSerializer := message.NewVectorIndexSerializer(ns.Pool())

	makeRootNode := func(keys, values [][]byte, subtrees []uint64, level int) (ProximityMap, error) {
		rootMsg := vectorIndexSerializer.Serialize(keys, values, subtrees, level)
		rootNode, err := tree.NodeFromBytes(rootMsg)
		if err != nil {
			return ProximityMap{}, err
		}
		_, err = ns.Write(ctx, rootNode)
		if err != nil {
			return ProximityMap{}, err
		}

		return NewProximityMap(ctx, ns, rootNode, keyDesc, valDesc, distanceType), nil
	}

	// Check if index is empty.
	if len(keys) == 0 {
		return makeRootNode(nil, nil, nil, 0)
	}

	// Step 1: Create `levelMap`, a map from (indexLevel, keyBytes) -> values
	// We want the index to be sorted first by level (descending), so currently we store the level in the map as
	// 255 - the actual level. TODO: Use a reverse iterator instead.
	mutableLevelMap, err := makeLevelMap(ctx, ns, keys, values, valDesc, logChunkSize)
	if err != nil {
		return ProximityMap{}, err
	}
	levelMapIter, err := mutableLevelMap.IterAll(ctx)
	if err != nil {
		return ProximityMap{}, err
	}

	// Step 2: Create `pathMaps`, a list of maps, each corresponding to a different level of the ProximityMap

	// The first element of levelMap tells us the height of the tree.
	levelMapKey, levelMapValue, err := levelMapIter.Next(ctx)
	if err != nil {
		return ProximityMap{}, err
	}
	maxLevel, _ := mutableLevelMap.keyDesc.GetUint8(0, levelMapKey)
	maxLevel = 255 - maxLevel

	if maxLevel == 0 {
		// index is a single node.
		// assuming that the keys are already sorted, we can return them unmodified.
		return makeRootNode(keys, values, nil, 0)
	}

	// Create every val.TupleBuilder and MutableMap that we will need
	// pathMaps[i] is the pathMap for depth i (and level maxLevel - i)
	pathMaps, keyTupleBuilders, prefixTupleBuilders, err := createInitialPathMaps(ctx, ns, valDesc, maxLevel)

	// Next, visit each key-value pair in decreasing order of level / increasing order of depth.
	// When visiting a pair from depth `i`, we use each of the previous `i` pathMaps to compute a path of `i` index keys.
	// This path dictate's that pair's location in the final ProximityMap.
	for {
		level, _ := mutableLevelMap.keyDesc.GetUint8(0, levelMapKey)
		level = 255 - level // we currently store the level as 255 - the actual level for sorting purposes.
		depth := int(maxLevel - level)

		keyTupleBuilder := keyTupleBuilders[level]
		// Compute the path that this row will have in the vector index, starting with the keys with the highest levels.
		// If the highest level is N, then a key at level L will have a path consisting of N-L vector hashes.
		// This path is computed in steps.
		var hashPath []hash.Hash
		keyToInsert, _ := mutableLevelMap.keyDesc.GetBytes(1, levelMapKey)
		vectorHashToInsert, _ := keyDesc.GetJSONAddr(0, keyToInsert)
		vectorToInsert, err := getVectorFromHash(ctx, ns, vectorHashToInsert)
		if err != nil {
			return ProximityMap{}, err
		}
		for pathColumn := 0; pathColumn < depth; pathColumn++ {
			prefixTupleBuilder := prefixTupleBuilders[int(maxLevel)-pathColumn]
			pathMap := pathMaps[int(maxLevel)-pathColumn]
			for tupleElem := 0; tupleElem < pathColumn; tupleElem++ {
				prefixTupleBuilder.PutJSONAddr(tupleElem, hashPath[tupleElem])
			}
			prefixTuple := prefixTupleBuilder.Build(ns.Pool())

			prefixRange := PrefixRange(prefixTuple, prefixTupleBuilder.Desc)
			pathMapIter, err := pathMap.IterRange(ctx, prefixRange)
			if err != nil {
				return ProximityMap{}, err
			}
			var candidateVectorHash hash.Hash
			if pathColumn == 0 {
				pathMapKey, _, err := pathMapIter.Next(ctx)
				if err != nil {
					return ProximityMap{}, err
				}
				originalKey, _ := pathMap.keyDesc.GetBytes(pathColumn, pathMapKey)
				candidateVectorHash, _ = keyDesc.GetJSONAddr(0, originalKey)
			} else {
				candidateVectorHash = hashPath[pathColumn-1]
			}

			candidateVector, err := getVectorFromHash(ctx, ns, candidateVectorHash)
			if err != nil {
				return ProximityMap{}, err
			}
			closestVectorHash := candidateVectorHash
			closestDistance, err := distanceType.Eval(vectorToInsert, candidateVector)
			if err != nil {
				return ProximityMap{}, err
			}

			for {
				pathMapKey, _, err := pathMapIter.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					return ProximityMap{}, err
				}
				originalKey, _ := pathMap.keyDesc.GetBytes(pathColumn, pathMapKey)
				candidateVectorHash, _ := keyDesc.GetJSONAddr(0, originalKey)
				candidateVector, err = getVectorFromHash(ctx, ns, candidateVectorHash)
				if err != nil {
					return ProximityMap{}, err
				}
				candidateDistance, err := distanceType.Eval(vectorToInsert, candidateVector)
				if err != nil {
					return ProximityMap{}, err
				}
				if candidateDistance < closestDistance {
					closestVectorHash = candidateVectorHash
					closestDistance = candidateDistance
				}
			}

			hashPath = append(hashPath, closestVectorHash)

		}

		for i, h := range hashPath {
			keyTupleBuilder.PutJSONAddr(i, h)
		}
		keyTupleBuilder.PutByteString(depth, keyToInsert)

		err = pathMaps[level].Put(ctx, keyTupleBuilder.Build(ns.Pool()), levelMapValue)
		if err != nil {
			return ProximityMap{}, err
		}

		levelMapKey, levelMapValue, err = levelMapIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ProximityMap{}, err
		}

	}

	// Step 3: Create an iter over each `pathMap` created in the previous step, and walk the shape of the final ProximityMap,
	// generating Nodes as we go.

	var chunker *vectorIndexChunker
	for i, pathMap := range pathMaps[:len(pathMaps)-1] {
		chunker, err = newVectorIndexChunker(ctx, pathMap, int(maxLevel)-(i), chunker)
		if err != nil {
			return ProximityMap{}, err
		}
	}
	rootPathMap := pathMaps[len(pathMaps)-1]
	topLevelPathMapIter, err := rootPathMap.IterAll(ctx)
	if err != nil {
		return ProximityMap{}, err
	}
	var topLevelKeys [][]byte
	var topLevelValues [][]byte
	var topLevelSubtrees []uint64
	for {
		key, value, err := topLevelPathMapIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ProximityMap{}, err
		}
		originalKey, _ := rootPathMap.keyDesc.GetBytes(0, key)
		path, _ := keyDesc.GetJSONAddr(0, originalKey)
		_, nodeCount, nodeHash, err := chunker.Next(ctx, ns, vectorIndexSerializer, path, originalKey, value, int(maxLevel)-1, 1, keyDesc)
		if err != nil {
			return ProximityMap{}, err
		}
		topLevelKeys = append(topLevelKeys, originalKey)
		topLevelValues = append(topLevelValues, nodeHash[:])
		topLevelSubtrees = append(topLevelSubtrees, nodeCount)
	}
	return makeRootNode(topLevelKeys, topLevelValues, topLevelSubtrees, int(maxLevel))
}

// makeLevelMap creates a prolly map where the key is prefixed by the maximum level of that row in the corresponding ProximityMap.
func makeLevelMap(ctx context.Context, ns tree.NodeStore, keys [][]byte, values [][]byte, valDesc val.TupleDesc, logChunkSize uint8) (*MutableMap, error) {
	levelMapKeyDesc := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint8Enc, Nullable: false},
		val.Type{Enc: val.ByteStringEnc, Nullable: false},
	)

	emptyLevelMap, err := NewMapFromTuples(ctx, ns, levelMapKeyDesc, valDesc)
	if err != nil {
		return nil, err
	}
	mutableLevelMap := newMutableMap(emptyLevelMap)

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		keyLevel := tree.DeterministicHashLevel(logChunkSize, []byte(key))

		levelMapKeyBuilder := val.NewTupleBuilder(levelMapKeyDesc)
		levelMapKeyBuilder.PutUint8(0, 255-keyLevel)
		levelMapKeyBuilder.PutByteString(1, key)
		err = mutableLevelMap.Put(ctx, levelMapKeyBuilder.Build(ns.Pool()), values[i])
		if err != nil {
			return nil, err
		}
	}

	return mutableLevelMap, nil
}

// createInitialPathMaps creates a list of MutableMaps that will eventually store a single level of the corresponding ProximityMap
func createInitialPathMaps(ctx context.Context, ns tree.NodeStore, valDesc val.TupleDesc, maxLevel uint8) (pathMaps []*MutableMap, keyTupleBuilders, prefixTupleBuilders []*val.TupleBuilder, err error) {
	keyTupleBuilders = make([]*val.TupleBuilder, maxLevel+1)
	prefixTupleBuilders = make([]*val.TupleBuilder, maxLevel+1)
	pathMaps = make([]*MutableMap, maxLevel+1)

	// Make a type slice for the maximum depth pathMap: each other slice we need is a subslice of this one.
	pathMapKeyDescTypes := make([]val.Type, maxLevel+1)
	for i := uint8(0); i < maxLevel; i++ {
		pathMapKeyDescTypes[i] = val.Type{Enc: val.JSONAddrEnc, Nullable: false}
	}
	pathMapKeyDescTypes[maxLevel] = val.Type{Enc: val.ByteStringEnc, Nullable: false}

	for i := uint8(0); i <= maxLevel; i++ {
		pathMapKeyDesc := val.NewTupleDescriptor(pathMapKeyDescTypes[i:]...)

		emptyPathMap, err := NewMapFromTuples(ctx, ns, pathMapKeyDesc, valDesc)
		if err != nil {
			return nil, nil, nil, err
		}
		pathMaps[i] = newMutableMap(emptyPathMap)

		keyTupleBuilders[i] = val.NewTupleBuilder(pathMapKeyDesc)
		prefixTupleBuilders[i] = val.NewTupleBuilder(val.NewTupleDescriptor(pathMapKeyDescTypes[i:maxLevel]...))
	}

	return pathMaps, keyTupleBuilders, prefixTupleBuilders, nil
}

// vectorIndexChunker is a stateful chunker that iterates over |pathMap|, a map that contains an element
// for every key-value pair for a given level of a ProximityMap, and provides the path of keys to reach
// that pair from the root. It uses this iterator to build each of the ProximityMap nodes for that level.
type vectorIndexChunker struct {
	pathMap          *MutableMap
	pathMapIter      MapIter
	lastPathSegment  hash.Hash
	lastKey          []byte
	lastValue        []byte
	lastSubtreeCount uint64
	childChunker     *vectorIndexChunker
	atEnd            bool
}

func newVectorIndexChunker(ctx context.Context, pathMap *MutableMap, depth int, childChunker *vectorIndexChunker) (*vectorIndexChunker, error) {
	pathMapIter, err := pathMap.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	firstKey, firstValue, err := pathMapIter.Next(ctx)
	if err == io.EOF {
		// In rare situations, there aren't any vectors at a given level.
		return &vectorIndexChunker{
			pathMap:      pathMap,
			pathMapIter:  pathMapIter,
			childChunker: childChunker,
			atEnd:        true,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	lastPathSegment, _ := pathMap.keyDesc.GetJSONAddr(depth-1, firstKey)
	originalKey, _ := pathMap.keyDesc.GetBytes(depth, firstKey)
	return &vectorIndexChunker{
		pathMap:         pathMap,
		pathMapIter:     pathMapIter,
		childChunker:    childChunker,
		lastKey:         originalKey,
		lastValue:       firstValue,
		lastPathSegment: lastPathSegment,
		atEnd:           false,
	}, nil
}

func (c *vectorIndexChunker) Next(ctx context.Context, ns tree.NodeStore, serializer message.VectorIndexSerializer, parentPathSegment hash.Hash, parentKey val.Tuple, parentValue val.Tuple, level, depth int, originalKeyDesc val.TupleDesc) (tree.Node, uint64, hash.Hash, error) {
	indexMapKeys := [][]byte{parentKey}
	var indexMapValues [][]byte
	var indexMapSubtrees []uint64
	subtreeSum := uint64(0)
	if c.childChunker != nil {
		_, childCount, nodeHash, err := c.childChunker.Next(ctx, ns, serializer, parentPathSegment, parentKey, parentValue, level-1, depth+1, originalKeyDesc)
		if err != nil {
			return tree.Node{}, 0, hash.Hash{}, err
		}
		indexMapValues = append(indexMapValues, nodeHash[:])
		indexMapSubtrees = append(indexMapSubtrees, childCount)
		subtreeSum += childCount
	} else {
		indexMapValues = append(indexMapValues, parentValue)
		subtreeSum++
	}

	for {
		if c.atEnd || c.lastPathSegment != parentPathSegment {
			msg := serializer.Serialize(indexMapKeys, indexMapValues, indexMapSubtrees, level)
			node, err := tree.NodeFromBytes(msg)
			if err != nil {
				return tree.Node{}, 0, hash.Hash{}, err
			}
			nodeHash, err := ns.Write(ctx, node)
			return node, subtreeSum, nodeHash, err
		}
		vectorHash, _ := originalKeyDesc.GetJSONAddr(0, c.lastKey)
		if c.childChunker != nil {
			_, childCount, nodeHash, err := c.childChunker.Next(ctx, ns, serializer, vectorHash, c.lastKey, c.lastValue, level-1, depth+1, originalKeyDesc)
			if err != nil {
				return tree.Node{}, 0, hash.Hash{}, err
			}
			c.lastValue = nodeHash[:]
			indexMapSubtrees = append(indexMapSubtrees, childCount)
			subtreeSum += childCount
		} else {
			subtreeSum++
		}
		indexMapKeys = append(indexMapKeys, c.lastKey)
		indexMapValues = append(indexMapValues, c.lastValue)

		nextKey, nextValue, err := c.pathMapIter.Next(ctx)
		if err == io.EOF {
			c.atEnd = true
		} else if err != nil {
			return tree.Node{}, 0, hash.Hash{}, err
		} else {
			c.lastPathSegment, _ = c.pathMap.keyDesc.GetJSONAddr(depth-1, nextKey)
			c.lastKey, _ = c.pathMap.keyDesc.GetBytes(depth, nextKey)
			c.lastValue = nextValue
		}
	}
}

// Count returns the number of key-value pairs in the Map.
func (m ProximityMap) Count() (int, error) {
	return m.tuples.Count()
}

// Get searches for the key-value pair keyed by |key| and passes the results to the callback.
// If |key| is not present in the map, a nil key-value pair are passed.
func (m ProximityMap) Get(ctx context.Context, query interface{}, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.GetExact(ctx, query, cb)
}

func (m ProximityMap) GetClosest(ctx context.Context, query interface{}, cb tree.KeyValueDistanceFn[val.Tuple, val.Tuple], limit int) (err error) {
	return m.tuples.GetClosest(ctx, query, cb, limit)
}
