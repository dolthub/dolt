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

package skip

import (
	"hash"

	"github.com/cespare/xxhash"
)

const (
	maxHeight = uint64(5)
	highest   = maxHeight - 1

	nullID = nodeId(0)
)

type List struct {
	head   skipPointer
	nodes  map[nodeId]skipNode
	hasher hash.Hash64
	cmp    ValueCmp
}

type skipNode struct {
	id       nodeId
	key, val []byte
	next     skipPointer
	height   uint64
}

type nodeId uint64

type skipPointer [maxHeight]nodeId

type ValueCmp func(left, right []byte) int

func NewSkipList(cmp ValueCmp) *List {
	nodes := make(map[nodeId]skipNode, 128)

	nodes[nullID] = skipNode{
		id:  nullID,
		key: nil, val: nil,
		height: maxHeight,
	}

	return &List{
		nodes:  nodes,
		hasher: xxhash.New(),
		cmp:    cmp,
	}
}

func (l *List) Count() int {
	return len(l.nodes) - 1
}

func (l *List) Has(key []byte) (ok bool) {
	_, ok = l.Get(key)
	return
}

func (l *List) Get(key []byte) (val []byte, ok bool) {
	var n skipNode
	n, ok = l.nodes[getHash(l.hasher, key)]
	if ok {
		val = n.val
	}
	return
}

func (l *List) Put(key, val []byte) {
	if key == nil {
		panic("key must be non-nil")
	}

	node := makeNode(l.hasher, key, val)

	if l.Has(key) {
		// we already have an entry for |key|,
		// so we can update in-place
		l.putNode(node)
		return
	}

	first := l.firstNode()
	// check if |node| is the new first
	if l.compare(node, first) <= 0 {
		for h := uint64(0); h <= node.height; h++ {
			node.next[h] = l.head[h]
			l.head[h] = node.id
		}
		l.putNode(node)
		return
	}

	// otherwise, search for an insertion point
	prev := first
	for h := int64(highest); h >= 0; h-- {

		next := l.getNode(prev.next[h])
		for l.compare(node, next) > 0 {
			prev = next
			next = l.getNode(next.next[h])
		}
		// invariant: |prev| < |node| < |curr|

		if node.height < uint64(h) {
			continue
		}

		node.next[h] = prev.next[h]
		prev.next[h] = node.id
		l.putNode(prev)
	}
	l.putNode(node)

	return
}

func (l *List) Iter(cb func(key, val []byte)) {
	node := l.firstNode()
	for node.id != nullID {
		cb(node.key, node.val)
		node = l.getNode(node.next[0])
	}
}

func (l List) firstNode() skipNode {
	return l.getNode(l.head[0])
}

func (l List) getNode(id nodeId) skipNode {
	return l.nodes[id]
}

func (l List) putNode(node skipNode) {
	l.nodes[node.id] = node
}

func (l List) compare(left, right skipNode) int {
	if right.id == nullID {
		return -1
	}
	return l.cmp(left.key, right.key)
}

func makeNode(hasher hash.Hash64, key, val []byte) skipNode {
	id := getHash(hasher, key)
	return skipNode{
		id:     id,
		key:    key,
		val:    val,
		height: getHeight(id),
	}
}

func getHash(h hash.Hash64, key []byte) (id nodeId) {
	id = nullID
	for id == nullID {
		// if we roll |nullID|, keep hashing
		_, _ = h.Write(key)
		id = nodeId(h.Sum64())
	}
	h.Reset()
	return
}

const (
	pattern1 = uint64(1<<2 - 1)
	pattern2 = uint64(1<<4 - 1)
	pattern3 = uint64(1<<6 - 1)
	pattern4 = uint64(1<<8 - 1)
)

func getHeight(id nodeId) (h uint64) {
	patterns := []uint64{
		pattern1,
		pattern2,
		pattern3,
		pattern4,
	}

	for _, pat := range patterns {
		if uint64(id)&pat != pat {
			break
		}
		h++
	}

	return
}
