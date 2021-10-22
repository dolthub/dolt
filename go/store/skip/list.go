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
	"math"
	"math/rand"
)

const (
	maxCount = math.MaxUint16 - 1

	maxHeight = uint8(5)
	highest   = maxHeight - 1

	nullID = nodeId(0)
)

type nodeId uint16

type skipPointer [maxHeight]nodeId

type skipNode struct {
	id       nodeId
	key, val []byte

	height uint8
	next   skipPointer
}

type ValueCmp func(left, right []byte) int

type List struct {
	head  skipPointer
	nodes []skipNode
	cmp   ValueCmp
	src   rand.Source
}

func NewSkipList(cmp ValueCmp) (l *List) {
	l = &List{
		// todo(andy): buffer pool
		nodes: make([]skipNode, 1, 128),
		cmp:   cmp,
		src:   rand.NewSource(0),
	}

	// initialize sentinal node
	l.nodes[nullID] = skipNode{
		id:  nullID,
		key: nil, val: nil,
		height: maxHeight,
	}

	return
}

func (l *List) Count() int {
	return len(l.nodes) - 1
}

func (l *List) Full() bool {
	return l.Count() >= maxCount
}

func (l *List) Has(key []byte) (ok bool) {
	_, ok = l.Get(key)
	return
}

func (l *List) Get(key []byte) (val []byte, ok bool) {
	ptr := l.head
	var curr skipNode

	for h := int64(highest); h >= 0; h-- {
		curr = l.getNode(ptr[h])
		for l.compareKeys(key, curr.key) > 0 {
			ptr = curr.next
			curr = l.getNode(ptr[h])
		}
	}

	if l.compareKeys(key, curr.key) == 0 {
		val, ok = curr.val, true
	}
	return
}

func (l *List) Put(key, val []byte) {
	if key == nil {
		panic("key must be non-nil")
	}

	next := l.head
	var prev skipNode
	var breadcrumbs skipPointer

	for h := int(highest); h >= 0; h-- {
		curr := l.getNode(next[h])
		for l.compareKeys(key, curr.key) > 0 {
			prev = curr
			next = curr.next
			curr = l.getNode(next[h])
		}
		// prev.key < key <= curr.key

		if l.compareKeys(key, curr.key) == 0 {
			// in-place update
			curr.val = val
			l.putNode(curr)
			return
		}

		// save our steps
		breadcrumbs[h] = prev.id
	}

	insert := l.makeNode(key, val)
	for h := uint8(0); h <= insert.height; h++ {
		// if |insert| is less than |head| for this level,
		// then update l.head
		head := l.getNode(l.head[h])
		if l.compare(insert, head) < 0 {
			l.head[h] = insert.id
			insert.next[h] = head.id
			continue
		}

		// otherwise, splice in |insert| at breadcrumbs[h]
		prev := l.getNode(breadcrumbs[h])
		insert.next[h] = prev.next[h]
		prev.next[h] = insert.id
		l.putNode(prev)
	}
	l.putNode(insert)

	return
}

func (l *List) Iter(cb func(key, val []byte)) {
	node := l.firstNode()
	for node.id != nullID {
		cb(node.key, node.val)
		node = l.getNode(node.next[0])
	}
}

func (l *List) firstNode() skipNode {
	return l.getNode(l.head[0])
}

func (l *List) getNode(id nodeId) skipNode {
	return l.nodes[id]
}

func (l *List) putNode(node skipNode) {
	l.nodes[node.id] = node
}

func (l *List) compare(left, right skipNode) int {
	return l.compareKeys(left.key, right.key)
}

func (l *List) compareKeys(left, right []byte) int {
	if right == nil {
		// |right| is sentinel key
		return -1
	}
	return l.cmp(left, right)
}

func (l *List) makeNode(key, val []byte) (n skipNode) {
	n = skipNode{
		id:     nodeId(len(l.nodes)),
		key:    key,
		val:    val,
		height: rollHeight(l.src),
	}
	l.nodes = append(l.nodes, n)

	return
}

const (
	pattern1 = uint64(1<<3 - 1)
	pattern2 = uint64(1<<6 - 1)
	pattern3 = uint64(1<<9 - 1)
	pattern4 = uint64(1<<12 - 1)
)

func rollHeight(r rand.Source) (h uint8) {
	roll := r.Int63()
	patterns := []uint64{
		pattern1,
		pattern2,
		pattern3,
		pattern4,
	}

	for _, pat := range patterns {
		if uint64(roll)&pat != pat {
			break
		}
		h++
	}

	return
}
