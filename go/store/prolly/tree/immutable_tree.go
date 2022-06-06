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

package tree

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/store/prolly/message"
)

const defaultFixedChunkLength = 4000

var ErrInvalidChunkSize = errors.New("invalid chunkSize; value must be > 1")

// buildImmutableTree writes the contents of |reader| as an append-only
// tree, returning the root node or an error if applicable. |chunkSize|
// fixes the split size of leaf and intermediate node chunks.
func buildImmutableTree(ctx context.Context, r io.Reader, ns NodeStore, S message.Serializer, chunkSize int) (Node, error) {
	if chunkSize <= 1 {
		return Node{}, ErrInvalidChunkSize
	}

	var levels [][]novelNode
	var levelCnts []int
	var finalize bool

	// We use lookahead to check whether the reader has
	// more bytes. The reader will only EOF when reading
	// zero bytes into the lookahead buffer, but we want
	// to know at the beginning of a loop whether we are
	// finished.
	lookahead := make([]byte, chunkSize)
	lookaheadN, err := r.Read(lookahead)
	if err != nil {
		return Node{}, err
	}

	buf := make([]byte, chunkSize)
	for {
		copy(buf, lookahead)
		curN := lookaheadN
		lookaheadN, err = r.Read(lookahead)
		if err == io.EOF {
			finalize = true
		} else if err != nil {
			return Node{}, err
		}

		novel, err := _newLeaf(ctx, ns, S, buf[:curN])
		if err != nil {
			return Node{}, err
		}

		i := 0
		for {
			// Three cases for building tree
			// 1) reached new level => create new level
			// 2) add novel node to current level
			// 3) we didn't fill the current level => break
			// 4) we filled current level, chunk and recurse into parent
			//
			// Two cases for finalizing tree
			// 1) we haven't hit root, so we add the final chunk, finalize level, and continue upwards
			// 2) we overshot root finalizing chunks, and we return the single root in the lower level
			if i > len(levels)-1 {
				levels = append(levels, make([]novelNode, chunkSize))
				levelCnts = append(levelCnts, 0)
			}

			levels[i][levelCnts[i]] = novel
			levelCnts[i]++
			if levelCnts[i] < chunkSize {
				// current level is not full
				if !finalize {
					// only continue and chunk this level if finalizing all in-progress nodes
					break
				}
			}

			nodes := levels[i][:levelCnts[i]]
			if len(nodes) == 1 && i == len(levels)-1 {
				// this is necessary and only possible if we're finalizing
				// note: this is the only non-error return
				return nodes[0].node, nil
			}

			// chunk the current level
			novel, err = _newInternal(ctx, ns, S, nodes, i+1, chunkSize)
			if err != nil {
				return Node{}, err
			}
			levelCnts[i] = 0
			i++
		}
	}
}

func _newInternal(ctx context.Context, ns NodeStore, s message.Serializer, nodes []novelNode, level int, chunkSize int) (novelNode, error) {
	keys := make([][]byte, len(nodes))
	vals := make([][]byte, len(nodes))
	subtrees := make([]uint64, len(nodes))
	treeCnt := uint64(0)
	for i := range nodes {
		keys[i] = []byte{0}
		vals[i] = nodes[i].addr[:]
		subtrees[i] = nodes[i].treeCount
		treeCnt += nodes[i].treeCount
	}
	msg := s.Serialize(keys, vals, subtrees, level)
	node := NodeFromBytes(msg)
	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}
	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   []byte{0},
		treeCount: treeCnt,
	}, nil
}

func _newLeaf(ctx context.Context, ns NodeStore, s message.Serializer, buf []byte) (novelNode, error) {
	msg := s.Serialize([][]byte{{0}}, [][]byte{buf}, []uint64{1}, 0)
	node := NodeFromBytes(msg)
	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}
	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   []byte{0},
		treeCount: 1,
	}, nil
}
