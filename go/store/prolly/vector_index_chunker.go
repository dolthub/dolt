// Copyright 2025 Dolthub, Inc.
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
	"bytes"
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// vectorIndexChunker is a stateful chunker that iterates over |pathMap|, a map that contains an element
// for every key-value pair for a given level of a ProximityMap, and provides the path of keys to reach
// that pair from the root. It uses this iterator to build each of the ProximityMap nodes for that level.
// A linked list of N vectorIndexChunkers is used in order to build a vector index with N levels.
type vectorIndexChunker struct {
	pathMapIter      MapIter
	pathMap          *MutableMap
	childChunker     *vectorIndexChunker
	lastKey          []byte
	lastValue        []byte
	lastSubtreeCount uint64
	// lastPathSegment is the last observed parent key. When Next() is called with a different value for |parentPathSegment|,
	// we know that the chunker needs to end the previous chunk and start a new one.
	lastPathSegment []byte
	atEnd           bool
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
	lastPathSegment, _ := pathMap.keyDesc.GetBytes(pathMap.keyDesc.Count()-2, firstKey)
	originalKey, _ := pathMap.keyDesc.GetBytes(pathMap.keyDesc.Count()-1, firstKey)
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

// Next produces the next tree node for the corresponding level of the tree.
func (c *vectorIndexChunker) Next(ctx context.Context, ns tree.NodeStore, serializer message.VectorIndexSerializer, parentPathSegment []byte, level, depth int, originalKeyDesc *val.TupleDesc) (tree.Node, uint64, hash.Hash, error) {
	var indexMapKeys [][]byte
	var indexMapValues [][]byte
	var indexMapSubtrees []uint64
	subtreeSum := uint64(0)

	for {
		if c.atEnd || !bytes.Equal(c.lastPathSegment, parentPathSegment) {
			msg := serializer.Serialize(indexMapKeys, indexMapValues, indexMapSubtrees, level)
			node, _, err := tree.NodeFromBytes(msg)
			if err != nil {
				return tree.Node{}, 0, hash.Hash{}, err
			}
			nodeHash, err := ns.Write(ctx, node)
			return node, subtreeSum, nodeHash, err
		}
		if c.childChunker != nil {
			// This chunker isn't chunking a leaf node. To insert the next key-value pair, we call Next() on the child chunker, which produces
			// a node one level down, that will be pointed to by this node.
			_, childCount, nodeHash, err := c.childChunker.Next(ctx, ns, serializer, c.lastKey, level-1, depth+1, originalKeyDesc)
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
			// nextValue is a pathMap value tuple: it contains a primary key from the underlying table.
			// nextKey is a pathMap key tuple: it contains a field for each edge in the final index graph that connects
			// the root to |nextValue|, ending with the vector corresponding to the primary key in |nextValue|.
			// This chunker stores that vector in |c.lastKey|, so that it can write it into the index on the subsequent call.
			// It also stores the direct parent vector. When the direct parent vector changes, we use that as an indicator
			// To finish one chunk and begin the next one.
			c.lastPathSegment, _ = c.pathMap.keyDesc.GetBytes(c.pathMap.keyDesc.Count()-2, nextKey)
			c.lastKey, _ = c.pathMap.keyDesc.GetBytes(c.pathMap.keyDesc.Count()-1, nextKey)
			c.lastValue = nextValue
		}
	}
}
