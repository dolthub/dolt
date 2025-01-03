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
	"io"
	"iter"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// ProximityMap wraps a tree.ProximityMap but operates on typed Tuples instead of raw bytestrings.
// A ProximityMap is like a Map, except that walking the tree does not produce a sorted order. Instead, each key
// is stored such that it is closer to its parent key than any of its uncle keys, according to a distance function
// defined on the tree.ProximityMap
type ProximityMap struct {
	tuples       tree.ProximityMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc      val.TupleDesc
	valDesc      val.TupleDesc
	logChunkSize uint8
}

// MutateInterface converts the map to a MutableMapInterface
func (m ProximityMap) MutateInterface() MutableMapInterface {
	return newProximityMutableMap(m)
}

func (m ProximityMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return m.tuples.WalkNodes(ctx, cb)
}

func (m ProximityMap) Node() tree.Node {
	return m.tuples.Root
}

func (m ProximityMap) HashOf() hash.Hash {
	return m.tuples.HashOf()
}

var _ MapInterface = ProximityMap{}

// Count returns the number of key-value pairs in the Map.
func (m ProximityMap) Count() (int, error) {
	return m.tuples.Count()
}

func (m ProximityMap) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m ProximityMap) NodeStore() tree.NodeStore {
	return m.tuples.NodeStore
}

func (m ProximityMap) ValDesc() val.TupleDesc {
	return m.valDesc
}

func (m ProximityMap) KeyDesc() val.TupleDesc {
	return m.keyDesc
}

func (m ProximityMap) Pool() pool.BuffPool {
	return m.tuples.NodeStore.Pool()
}

func (m ProximityMap) IterAll(ctx context.Context) (MapIter, error) {
	return m.tuples.IterAll(ctx)
}

// Get searches for key-value pairs keyed by |query| and passes the results to the callback.
// If |query| is not present in the map, a nil key-value pair are passed.
func (m ProximityMap) Get(ctx context.Context, query val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.Get(ctx, query, cb)
}

// Has returns true is |key| is present in the Map.
func (m ProximityMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return m.tuples.Has(ctx, key)
}

// GetClosest returns a MapIter that produces the |limit| closest key-value pairs to the provided query key.
func (m ProximityMap) GetClosest(ctx context.Context, query interface{}, limit int) (mapIter MapIter, err error) {
	kvPairs := make([]kvPair, 0, limit)
	cb := func(key val.Tuple, value val.Tuple, distance float64) error {
		kvPairs = append(kvPairs, kvPair{key, value})
		return nil
	}
	err = m.tuples.GetClosest(ctx, query, cb, limit)
	if err != nil {
		return nil, err
	}
	return &proximityMapIter{
		m.keyDesc, m.valDesc, kvPairs, 0,
	}, nil
}

type kvPair struct {
	key, value val.Tuple
}

type proximityMapIter struct {
	keyDesc, valueDesc val.TupleDesc
	kvPairs            []kvPair
	i                  int
}

var _ MapIter = (*proximityMapIter)(nil)

func (p *proximityMapIter) Next(ctx context.Context) (k val.Tuple, v val.Tuple, err error) {
	if p.i >= len(p.kvPairs) {
		return nil, nil, io.EOF
	}
	pair := p.kvPairs[p.i]
	k = pair.key
	v = pair.value
	p.i++
	return
}

// NewProximityMap creates a new ProximityMap from a supplied root node.
func NewProximityMap(ctx context.Context, ns tree.NodeStore, node tree.Node, keyDesc val.TupleDesc, valDesc val.TupleDesc, distanceType vector.DistanceType, logChunkSize uint8) ProximityMap {
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
		tuples:       tuples,
		keyDesc:      keyDesc,
		valDesc:      valDesc,
		logChunkSize: logChunkSize,
	}
}

var levelMapKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint8Enc, Nullable: false},
	val.Type{Enc: val.ByteStringEnc, Nullable: false},
)

// NewProximityMapBuilder creates a new ProximityMap from a given list of key-value pairs.
func NewProximityMapBuilder(ctx context.Context, ns tree.NodeStore, distanceType vector.DistanceType, keyDesc val.TupleDesc, valDesc val.TupleDesc, logChunkSize uint8) (ProximityMapBuilder, error) {

	emptyLevelMap, err := NewMapFromTuples(ctx, ns, levelMapKeyDesc, valDesc)
	if err != nil {
		return ProximityMapBuilder{}, err
	}
	mutableLevelMap := newMutableMap(emptyLevelMap)
	return ProximityMapBuilder{
		ns:                    ns,
		vectorIndexSerializer: message.NewVectorIndexSerializer(ns.Pool(), logChunkSize),
		distanceType:          distanceType,
		keyDesc:               keyDesc,
		valDesc:               valDesc,
		logChunkSize:          logChunkSize,
		maxLevel:              0,
		levelMap:              mutableLevelMap,
	}, nil
}

// ProximityMapBuilder is used to create a ProximityMap.
// Each node has an average of 2^|logChunkSize| key-value pairs.
type ProximityMapBuilder struct {
	ns                    tree.NodeStore
	vectorIndexSerializer message.VectorIndexSerializer
	distanceType          vector.DistanceType
	keyDesc, valDesc      val.TupleDesc
	logChunkSize          uint8

	maxLevel uint8
	levelMap *MutableMap
}

// Insert adds a new key-value pair to the ProximityMap under construction.
func (b *ProximityMapBuilder) Insert(ctx context.Context, key, value []byte) error {
	keyLevel := tree.DeterministicHashLevel(b.logChunkSize, key)
	if keyLevel > b.maxLevel {
		b.maxLevel = keyLevel
	}

	levelMapKeyBuilder := val.NewTupleBuilder(levelMapKeyDesc)
	levelMapKeyBuilder.PutUint8(0, 255-keyLevel)
	levelMapKeyBuilder.PutByteString(1, key)
	return b.levelMap.Put(ctx, levelMapKeyBuilder.Build(b.ns.Pool()), value)
}

// InsertAtLevel inserts into a proximity map when the level for a key is already known
// This is called when an existing tree is being modified, and can skip the level calculation.
func (b *ProximityMapBuilder) InsertAtLevel(ctx context.Context, key, value []byte, keyLevel uint8) error {
	/*
		// Uncomment this check when debugging
		if uint8(level) != tree.DeterministicHashLevel(b.logChunkSize, key) {
			panic("wrong level")
		}
	*/

	if keyLevel > b.maxLevel {
		b.maxLevel = keyLevel
	}
	levelMapKeyBuilder := val.NewTupleBuilder(levelMapKeyDesc)
	levelMapKeyBuilder.PutUint8(0, 255-keyLevel)
	levelMapKeyBuilder.PutByteString(1, key)
	return b.levelMap.Put(ctx, levelMapKeyBuilder.Build(b.ns.Pool()), value)
}

// makeRootNode creates a ProximityMap with a root node constructed from the provided parameters.
func (b *ProximityMapBuilder) makeRootNode(ctx context.Context, keys, values [][]byte, subtrees []uint64, level int) (ProximityMap, error) {
	rootMsg := b.vectorIndexSerializer.Serialize(keys, values, subtrees, level)
	rootNode, _, err := tree.NodeFromBytes(rootMsg)
	if err != nil {
		return ProximityMap{}, err
	}
	_, err = b.ns.Write(ctx, rootNode)
	if err != nil {
		return ProximityMap{}, err
	}

	return NewProximityMap(ctx, b.ns, rootNode, b.keyDesc, b.valDesc, b.distanceType, b.logChunkSize), nil
}

func (b *ProximityMapBuilder) Flush(ctx context.Context) (ProximityMap, error) {
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

	flushedLevelMap, err := b.levelMap.Map(ctx)
	if err != nil {
		return ProximityMap{}, err
	}

	levelMapSize, err := flushedLevelMap.Count()
	if err != nil {
		return ProximityMap{}, err
	}

	if levelMapSize == 0 {
		// Index is empty.
		return b.makeRootNode(ctx, nil, nil, nil, 0)
	}

	// Step 1: Create `levelMap`, a map from (indexLevel, keyBytes) -> values
	// We want the index to be sorted by level (descending), so currently we store the level in the map as
	// 255 - the actual level.

	// In the future, if MutableMap supports a ReverseIter function, we can use that instead.

	if b.maxLevel == 0 {
		// index is a single node.
		// assuming that the keys are already sorted, we can return them unmodified.
		levelMapIter, err := b.levelMap.IterAll(ctx)
		if err != nil {
			return ProximityMap{}, err
		}
		var keys, values [][]byte
		for {
			key, value, err := levelMapIter.Next(ctx)
			if err == io.EOF {
				break
			}
			originalKey, _ := levelMapKeyDesc.GetBytes(1, key)
			if err != nil {
				return ProximityMap{}, err
			}
			keys = append(keys, originalKey)
			values = append(values, value)
		}
		return b.makeRootNode(ctx, keys, values, nil, 0)
	}

	// Step 2: Create `pathMaps`, a list of maps, each corresponding to a different level of the ProximityMap
	pathMaps, err := b.makePathMaps(ctx, b.levelMap)
	if err != nil {
		return ProximityMap{}, err
	}

	// Step 3: Create an iter over each `pathMap` created in the previous step, and walk the shape of the final ProximityMap,
	// generating Nodes as we go.
	return b.makeProximityMapFromPathMaps(ctx, pathMaps)
}

// makePathMaps creates a set of prolly maps, each of which corresponds to a different level in the to-be-built ProximityMap
func (b *ProximityMapBuilder) makePathMaps(ctx context.Context, mutableLevelMap *MutableMap) ([]*MutableMap, error) {
	levelMapIter, err := mutableLevelMap.IterAll(ctx)
	if err != nil {
		return nil, err
	}

	// The first element of levelMap tells us the height of the tree.
	levelMapKey, levelMapValue, err := levelMapIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	maxLevel, _ := mutableLevelMap.keyDesc.GetUint8(0, levelMapKey)
	maxLevel = 255 - maxLevel

	// Create every val.TupleBuilder and MutableMap that we will need
	// pathMaps[i] is the pathMap for level i (and depth maxLevel - i)
	pathMaps, keyTupleBuilder, prefixTupleBuilder, err := b.createInitialPathMaps(ctx, maxLevel)

	// Next, visit each key-value pair in decreasing order of level / increasing order of depth.
	// When visiting a pair from depth `i`, we use each of the previous `i` pathMaps to compute a path of `i` index keys.
	// This path dictate's that pair's location in the final ProximityMap.
	for {
		level, _ := mutableLevelMap.keyDesc.GetUint8(0, levelMapKey)
		level = 255 - level // we currently store the level as 255 - the actual level for sorting purposes.
		depth := int(maxLevel - level)

		// hashPath is a list of concatenated hashes, representing the sequence of closest vectors at each level of the tree.
		var hashPath []byte
		keyToInsert, _ := mutableLevelMap.keyDesc.GetBytes(1, levelMapKey)
		vectorHashToInsert, _ := b.keyDesc.GetJSONAddr(0, keyToInsert)
		vectorToInsert, err := getVectorFromHash(ctx, b.ns, vectorHashToInsert)
		if err != nil {
			return nil, err
		}
		// Compute the path that this row will have in the vector index, starting at the root.
		// A key-value pair at depth D will have a path D prior keys.
		// This path is computed in steps, by performing a lookup in each of the prior pathMaps.
		for pathDepth := 0; pathDepth < depth; pathDepth++ {
			lookupLevel := int(maxLevel) - pathDepth
			pathMap := pathMaps[lookupLevel]

			pathMapIter, err := b.getNextPathSegmentCandidates(ctx, pathMap, prefixTupleBuilder, hashPath)
			if err != nil {
				return nil, err
			}

			// Create an iterator that yields every candidate vector
			nextCandidate, stopIter := iter.Pull2(func(yield func(hash.Hash, error) bool) {
				for {
					pathMapKey, _, err := pathMapIter.Next(ctx)
					if err == io.EOF {
						return
					}
					if err != nil {
						yield(hash.Hash{}, err)
					}
					originalKey, _ := pathMap.keyDesc.GetBytes(1, pathMapKey)
					candidateVectorHash, _ := b.keyDesc.GetJSONAddr(0, originalKey)
					yield(candidateVectorHash, nil)
				}
			})
			defer stopIter()

			closestVectorHash, _ := b.getClosestVector(ctx, vectorToInsert, nextCandidate)

			hashPath = append(hashPath, closestVectorHash[:]...)
		}

		// Once we have the path for this key, we turn it into a tuple and add it to the next pathMap.
		keyTupleBuilder.PutByteString(0, hashPath)
		keyTupleBuilder.PutByteString(1, keyToInsert)

		keyTuple := keyTupleBuilder.Build(b.ns.Pool())
		err = pathMaps[level].Put(ctx, keyTuple, levelMapValue)
		if err != nil {
			return nil, err
		}

		// Since a key that appears at level N also appears at every previous level, we insert into those level maps too
		// Since level is unsigned, we can't write `for childLevel > 0` here.
		childLevel := level - 1
		if level > 0 {
			for {
				hashPath = append(hashPath, vectorHashToInsert[:]...)
				keyTupleBuilder.PutByteString(0, hashPath)
				keyTupleBuilder.PutByteString(1, keyToInsert)

				childKeyTuple := keyTupleBuilder.Build(b.ns.Pool())
				err = pathMaps[childLevel].Put(ctx, childKeyTuple, levelMapValue)
				if err != nil {
					return nil, err
				}

				if childLevel == 0 {
					break
				}
				childLevel--
			}
		}

		levelMapKey, levelMapValue, err = levelMapIter.Next(ctx)
		if err == io.EOF {
			return pathMaps, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// createInitialPathMaps creates a list of MutableMaps that will eventually store a single level of the to-be-built ProximityMap
func (b *ProximityMapBuilder) createInitialPathMaps(ctx context.Context, maxLevel uint8) (pathMaps []*MutableMap, keyTupleBuilder, prefixTupleBuilder *val.TupleBuilder, err error) {
	pathMaps = make([]*MutableMap, maxLevel+1)

	pathMapKeyDescTypes := []val.Type{{Enc: val.ByteStringEnc, Nullable: false}, {Enc: val.ByteStringEnc, Nullable: false}}

	pathMapKeyDesc := val.NewTupleDescriptor(pathMapKeyDescTypes...)

	emptyPathMap, err := NewMapFromTuples(ctx, b.ns, pathMapKeyDesc, b.valDesc)

	keyTupleBuilder = val.NewTupleBuilder(pathMapKeyDesc)
	prefixTupleBuilder = val.NewTupleBuilder(val.NewTupleDescriptor(pathMapKeyDescTypes[0]))

	for i := uint8(0); i <= maxLevel; i++ {

		if err != nil {
			return nil, nil, nil, err
		}
		pathMaps[i] = newMutableMap(emptyPathMap)
	}

	return pathMaps, keyTupleBuilder, prefixTupleBuilder, nil
}

// getNextPathSegmentCandidates takes a list of keys, representing a path into the ProximityMap from the root.
// It returns an iter over all possible keys that could be the next path segment.
func (b *ProximityMapBuilder) getNextPathSegmentCandidates(ctx context.Context, pathMap *MutableMap, prefixTupleBuilder *val.TupleBuilder, currentPath []byte) (MapIter, error) {
	prefixTupleBuilder.PutByteString(0, currentPath)
	prefixTuple := prefixTupleBuilder.Build(b.ns.Pool())

	prefixRange := PrefixRange(prefixTuple, prefixTupleBuilder.Desc)
	return pathMap.IterRange(ctx, prefixRange)
}

// getClosestVector iterates over a range of candidate vectors to determine which one is the closest to the target.
func (b *ProximityMapBuilder) getClosestVector(ctx context.Context, targetVector []float64, nextCandidate func() (candidate hash.Hash, err error, valid bool)) (hash.Hash, error) {
	// First call to nextCandidate is guaranteed to be valid because there's at least one vector in the set.
	// (non-root nodes inherit the first vector from their parent)
	candidateVectorHash, err, _ := nextCandidate()
	if err != nil {
		return hash.Hash{}, err
	}

	candidateVector, err := getVectorFromHash(ctx, b.ns, candidateVectorHash)
	if err != nil {
		return hash.Hash{}, err
	}
	closestVectorHash := candidateVectorHash
	closestDistance, err := b.distanceType.Eval(targetVector, candidateVector)
	if err != nil {
		return hash.Hash{}, err
	}

	for {
		candidateVectorHash, err, valid := nextCandidate()
		if err != nil {
			return hash.Hash{}, err
		}
		if !valid {
			return closestVectorHash, nil
		}
		candidateVector, err = getVectorFromHash(ctx, b.ns, candidateVectorHash)
		if err != nil {
			return hash.Hash{}, err
		}
		candidateDistance, err := b.distanceType.Eval(targetVector, candidateVector)
		if err != nil {
			return hash.Hash{}, err
		}
		if candidateDistance < closestDistance {
			closestVectorHash = candidateVectorHash
			closestDistance = candidateDistance
		}
	}
}

// makeProximityMapFromPathMaps builds a ProximityMap from a list of maps, each of which corresponds to a different tree level.
func (b *ProximityMapBuilder) makeProximityMapFromPathMaps(ctx context.Context, pathMaps []*MutableMap) (proximityMap ProximityMap, err error) {
	maxLevel := len(pathMaps) - 1

	// We create a chain of vectorIndexChunker objects, with the leaf row at the tail.
	// Because the root node has no parent, the logic is slightly different. We don't make a vectorIndexChunker for it.
	var chunker *vectorIndexChunker
	for _, pathMap := range pathMaps[:maxLevel] {
		chunker, err = newVectorIndexChunker(ctx, pathMap, chunker)
		if err != nil {
			return ProximityMap{}, err
		}
	}

	rootPathMap := pathMaps[maxLevel]
	topLevelPathMapIter, err := rootPathMap.IterAll(ctx)
	if err != nil {
		return ProximityMap{}, err
	}
	var topLevelKeys [][]byte
	var topLevelValues [][]byte
	var topLevelSubtrees []uint64
	for {
		key, _, err := topLevelPathMapIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ProximityMap{}, err
		}
		originalKey, _ := rootPathMap.keyDesc.GetBytes(1, key)
		path, _ := b.keyDesc.GetJSONAddr(0, originalKey)
		_, nodeCount, nodeHash, err := chunker.Next(ctx, b.ns, b.vectorIndexSerializer, path, maxLevel-1, 1, b.keyDesc)
		if err != nil {
			return ProximityMap{}, err
		}
		topLevelKeys = append(topLevelKeys, originalKey)
		topLevelValues = append(topLevelValues, nodeHash[:])
		topLevelSubtrees = append(topLevelSubtrees, nodeCount)
	}
	return b.makeRootNode(ctx, topLevelKeys, topLevelValues, topLevelSubtrees, maxLevel)
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

func newVectorIndexChunker(ctx context.Context, pathMap *MutableMap, childChunker *vectorIndexChunker) (*vectorIndexChunker, error) {
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
	path, _ := pathMap.keyDesc.GetBytes(0, firstKey)
	lastPathSegment := hash.New(path[len(path)-20:])
	originalKey, _ := pathMap.keyDesc.GetBytes(1, firstKey)
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

func (c *vectorIndexChunker) Next(ctx context.Context, ns tree.NodeStore, serializer message.VectorIndexSerializer, parentPathSegment hash.Hash, level, depth int, originalKeyDesc val.TupleDesc) (tree.Node, uint64, hash.Hash, error) {
	var indexMapKeys [][]byte
	var indexMapValues [][]byte
	var indexMapSubtrees []uint64
	subtreeSum := uint64(0)

	for {
		if c.atEnd || c.lastPathSegment != parentPathSegment {
			msg := serializer.Serialize(indexMapKeys, indexMapValues, indexMapSubtrees, level)
			node, _, err := tree.NodeFromBytes(msg)
			if err != nil {
				return tree.Node{}, 0, hash.Hash{}, err
			}
			nodeHash, err := ns.Write(ctx, node)
			return node, subtreeSum, nodeHash, err
		}
		vectorHash, _ := originalKeyDesc.GetJSONAddr(0, c.lastKey)
		if c.childChunker != nil {
			_, childCount, nodeHash, err := c.childChunker.Next(ctx, ns, serializer, vectorHash, level-1, depth+1, originalKeyDesc)
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
			lastPath, _ := c.pathMap.keyDesc.GetBytes(0, nextKey)
			c.lastPathSegment = hash.New(lastPath[len(lastPath)-20:])
			c.lastKey, _ = c.pathMap.keyDesc.GetBytes(1, nextKey)
			c.lastValue = nextValue
		}
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
