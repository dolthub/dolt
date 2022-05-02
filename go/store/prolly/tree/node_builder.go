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
	"encoding/binary"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	nodeBuilderListSize = 256
)

// NodeBuilderFactory ProviderBean™
type NodeBuilderFactory[B NodeBuilder] func(level int) B

// NodeBuilder builds prolly tree Nodes.
type NodeBuilder interface {
	// StartNode initializes a NodeBuilder to start building a new Node.
	StartNode()

	// Count returns the number of key-value pairs in the NodeBuilder.
	Count() int

	// HasCapacity returns true if the NodeBuilder can fit the next pair.
	HasCapacity(key, value Item) bool

	// AddItems adds a key-value pair to the NodeBuilder.
	AddItems(key, value Item, subtree uint64)

	// Build constructs a new Node from the accumulated key-value pairs.
	Build(p pool.BuffPool) Node
}

type novelNode struct {
	node      Node
	addr      hash.Hash
	lastKey   Item
	treeCount uint64
}

func writeNewNode(ctx context.Context, ns NodeStore, bld NodeBuilder) (novelNode, error) {
	node := bld.Build(ns.Pool())

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

type SubtreeCounts []uint64

func (sc SubtreeCounts) Sum() (s uint64) {
	for _, count := range sc {
		s += count
	}
	return
}

func readSubtreeCounts(n int, buf []byte) (sc SubtreeCounts) {
	sc = make([]uint64, 0, n)
	for len(buf) > 0 {
		count, n := binary.Uvarint(buf)
		sc = append(sc, count)
		buf = buf[n:]
	}
	assertTrue(len(sc) == n)
	return
}

func WriteSubtreeCounts(sc SubtreeCounts) []byte {
	buf := make([]byte, len(sc)*binary.MaxVarintLen64)
	pos := 0
	for _, count := range sc {
		n := binary.PutUvarint(buf[pos:], count)
		pos += n
	}
	return buf[:pos]
}
