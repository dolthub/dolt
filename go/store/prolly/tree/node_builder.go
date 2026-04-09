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
	"fmt"
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
)

type novelNode struct {
	node      *Node
	lastKey   Item
	treeCount uint64
	addr      hash.Hash
}

func writeNewNode[S message.Serializer](ctx context.Context, ns NodeStore, bld *nodeBuilder[S]) (novelNode, error) {
	node, treeCnt, err := bld.build()
	if err != nil {
		return novelNode{}, err
	}

	addr, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}

	var lastKey Item
	if node.Count() > 0 {
		k := getLastKey(node)
		lastKey = ns.Pool().Get(uint64(len(k)))
		copy(lastKey, k)
	}

	otherTreeCnt, err := node.TreeCount()
	if treeCnt != uint64(otherTreeCnt) {
		panic(fmt.Sprintf("node tree count mismatch: %d != %d", treeCnt, otherTreeCnt))
	}

	return novelNode{
		addr:      addr,
		node:      node,
		lastKey:   lastKey,
		treeCount: treeCnt,
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
	serializer S
	keys       [][]byte
	values     [][]byte
	subtrees   subtreeCounts
	size       int
	level      int
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

func (nb *nodeBuilder[S]) build() (node *Node, treeCount uint64, err error) {
	msg := nb.serializer.Serialize(nb.keys, nb.values, nb.subtrees, nb.level)
	var fileID string
	node, fileID, err = NodeFromBytes(msg)
	if nb.level == 0 && fileID != serial.BlobFileID {
		treeCount = uint64(len(nb.keys))
	} else {
		treeCount = message.SumSubtrees(nb.subtrees)
	}
	nb.recycleBuffers()
	nb.size = 0
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
