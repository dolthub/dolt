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
type vectorIndexChunker struct {
	pathMapIter      MapIter
	pathMap          *MutableMap
	childChunker     *vectorIndexChunker
	lastKey          []byte
	lastValue        []byte
	lastSubtreeCount uint64
	lastPathSegment  []byte
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

func (c *vectorIndexChunker) Next(ctx context.Context, ns tree.NodeStore, serializer message.VectorIndexSerializer, parentPathSegment []byte, level, depth int, originalKeyDesc val.TupleDesc) (tree.Node, uint64, hash.Hash, error) {
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
			c.lastPathSegment, _ = c.pathMap.keyDesc.GetBytes(c.pathMap.keyDesc.Count()-2, nextKey)
			c.lastKey, _ = c.pathMap.keyDesc.GetBytes(c.pathMap.keyDesc.Count()-1, nextKey)
			c.lastValue = nextValue
		}
	}
}
