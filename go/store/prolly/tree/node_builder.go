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

	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	nodeBuilderListSize = 256
)

type novelNode struct {
	node      Node
	addr      hash.Hash
	lastKey   Item
	treeCount uint64
}

func writeNewNode[S message.Serializer](ctx context.Context, ns NodeStore, bld *nodeBuilder[S]) (novelNode, error) {
	node := bld.build()

	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}

	var lastKey Item
	if node.count > 0 {
		k := node.GetKey(int(node.count) - 1)
		lastKey = ns.Pool().Get(uint64(len(k)))
		copy(lastKey, k)
	}

	treeCount := uint64(node.TreeCount())

	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   lastKey,
		treeCount: treeCount,
	}, nil
}

func newNodeBuilder[S message.Serializer](serializer S, level int) *nodeBuilder[S] {
	return &nodeBuilder[S]{
		level:      level,
		serializer: serializer,
	}
}

type nodeBuilder[S message.Serializer] struct {
	keys, values [][]byte
	size, level  int
	subtrees     SubtreeCounts
	serializer   S
}

func (nb *nodeBuilder[S]) startNode() {
	nb.reset()
}

func (nb *nodeBuilder[S]) hasCapacity(key, value Item) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(message.MaxVectorOffset)
}

func (nb *nodeBuilder[S]) addItems(key, value Item, subtree uint64) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *nodeBuilder[S]) count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder[S]) build() (node Node) {
	msg := nb.serializer.Serialize(nb.keys, nb.values, nb.subtrees, nb.level)
	nb.reset()
	return NodeFromBytes(msg)
}

func (nb *nodeBuilder[S]) reset() {
	// buffers are copied, it's safe to re-use the memory.
	nb.keys = nb.keys[:0]
	nb.values = nb.values[:0]
	nb.size = 0
	nb.subtrees = nb.subtrees[:0]
}
