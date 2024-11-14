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
	"sync"

	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/hash"
)

type novelNode struct {
	node      Node
	addr      hash.Hash
	lastKey   Item
	treeCount uint64
}

func writeNewNode[S message.Serializer](ctx context.Context, ns NodeStore, bld *nodeBuilder[S]) (novelNode, error) {

	node, err := bld.build()
	if err != nil {
		return novelNode{}, err
	}

	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}

	var lastKey Item
	if node.count > 0 {
		k := getLastKey(node)
		lastKey = ns.Pool().Get(uint64(len(k)))
		copy(lastKey, k)
	}

	cnt, err := node.TreeCount()
	if err != nil {
		return novelNode{}, err
	}

	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   lastKey,
		treeCount: uint64(cnt),
	}, nil
}

func newNodeBuilder[S message.Serializer](serializer S, level int) (nb *nodeBuilder[S]) {
	nb = &nodeBuilder[S]{
		level:      level,
		serializer: serializer,
	}
	return
}

type nodeBuilder[S message.Serializer] struct {
	keys, values [][]byte
	size, level  int
	subtrees     subtreeCounts
	serializer   S
}

func (nb *nodeBuilder[S]) hasCapacity(key, value Item) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(message.MaxVectorOffset)
}

func (nb *nodeBuilder[S]) addItems(key, value Item, subtree uint64) {
	if nb.keys == nil {
		nb.keys = getItemSlices()
		nb.values = getItemSlices()
		nb.subtrees = getSubtreeSlice()
	}
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *nodeBuilder[S]) count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder[S]) build() (node Node, err error) {
	msg := nb.serializer.Serialize(nb.keys, nb.values, nb.subtrees, nb.level)
	nb.recycleBuffers()
	nb.size = 0
	node, _, err = NodeFromBytes(msg)
	return
}

func (nb *nodeBuilder[S]) recycleBuffers() {
	putItemSlices(nb.keys[:0])
	putItemSlices(nb.values[:0])
	putSubtreeSlice(nb.subtrees[:0])
	nb.keys = nil
	nb.values = nil
	nb.subtrees = nil
}

// todo(andy): replace with NodeStore.Pool()
const nodeBuilderListSize = 256

var itemsPool = sync.Pool{
	New: func() any {
		return make([][]byte, 0, nodeBuilderListSize)
	},
}

func getItemSlices() [][]byte {
	sl := itemsPool.Get().([][]byte)
	return sl[:0]
}

func putItemSlices(sl [][]byte) {
	itemsPool.Put(sl[:0])
}

var subtreePool = sync.Pool{
	New: func() any {
		return make([]uint64, 0, nodeBuilderListSize)
	},
}

func getSubtreeSlice() []uint64 {
	sl := subtreePool.Get().([]uint64)
	return sl[:0]
}

func putSubtreeSlice(sl []uint64) {
	subtreePool.Put(sl[:0])
}
