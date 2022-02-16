// Copyright 2021 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	nodeItemAccumulatorSz = 256
)

type novelNode struct {
	node    Node
	ref     hash.Hash
	lastKey nodeItem
}

func newNodeBuilder() *nodeBuilder {
	return &nodeBuilder{
		keys: make([]nodeItem, 0, nodeItemAccumulatorSz),
		vals: make([]nodeItem, 0, nodeItemAccumulatorSz),
	}
}

type nodeBuilder struct {
	keys, vals []nodeItem
	size       int
}

func (m *nodeBuilder) count() int {
	return len(m.keys)
}

func (m *nodeBuilder) hasCapacity(key, value nodeItem) bool {
	sum := m.size + len(key) + len(value)
	return sum <= int(maxVectorOffset)
}

func (m *nodeBuilder) append(key, value nodeItem) {
	m.keys = append(m.keys, key)
	m.vals = append(m.vals, value)
	m.size += len(key) + len(value)
}

func (m *nodeBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	m.keys = m.keys[:0]
	m.vals = m.vals[:0]
	m.size = 0
}

// writeNewNode creates a Node from the keys items in |sc.currentPair|,
// clears the keys items, then returns the new Node and a metaValue that
// points to it. The Node is always eagerly written.
func (m *nodeBuilder) writeNewNode(ctx context.Context, ns NodeStore, level int) (novelNode, error) {
	node := buildMapNode(ns.Pool(), level, 0, m.keys, m.vals)
	ref, err := ns.Write(ctx, node)
	if err != nil {
		return novelNode{}, err
	}

	if len(m.keys) == 0 {
		// empty leaf node
		return novelNode{}, nil
	}

	lastKey := val.Tuple(m.keys[len(m.keys)-1])
	lastKey = val.CloneTuple(ns.Pool(), lastKey)

	return novelNode{
		ref:     ref,
		node:    node,
		lastKey: nodeItem(lastKey),
	}, nil
}

func (m *nodeBuilder) getFirstRef() hash.Hash {
	return hash.New(m.vals[0])
}
