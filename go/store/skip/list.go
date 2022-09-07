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
	maxHeight  = 9
	maxCount   = math.MaxUint32 - 1
	sentinelId = nodeId(0)
)

type KeyOrder func(l, r []byte) (cmp int)

type SeekFn func(key []byte) (advance bool)

type List struct {
	nodes      []skipNode
	count      uint32
	checkpoint nodeId
	keyOrder   KeyOrder
}

type nodeId uint32

type skipPointer [maxHeight + 1]nodeId

type skipNode struct {
	key, val []byte

	id     nodeId
	next   skipPointer
	prev   nodeId
	height uint8
}

func NewSkipList(order KeyOrder) *List {
	nodes := make([]skipNode, 0, 8)

	// initialize sentinel node
	nodes = append(nodes, skipNode{
		id:  sentinelId,
		key: nil, val: nil,
		height: maxHeight,
		next:   skipPointer{},
		prev:   sentinelId,
	})

	return &List{
		nodes:      nodes,
		checkpoint: nodeId(1),
		keyOrder:   order,
	}
}

// Checkpoint records a checkpoint that can be reverted to.
func (l *List) Checkpoint() {
	l.checkpoint = l.nextNodeId()
}

// Revert reverts to the last recorded checkpoint.
func (l *List) Revert() {
	keepers := l.nodes[1:l.checkpoint]
	l.Truncate()
	for _, nd := range keepers {
		l.Put(nd.key, nd.val)
	}
}

// Truncate deletes all entries from the list.
func (l *List) Truncate() {
	l.nodes = l.nodes[:1]
	// point sentinel.prev at itself
	s := l.getNode(sentinelId)
	s.next = skipPointer{}
	s.prev = sentinelId
	l.updateNode(s)
	l.count = 0
}

func (l *List) Count() int {
	return int(l.count)
}

func (l *List) Has(key []byte) (ok bool) {
	_, ok = l.Get(key)
	return
}

func (l *List) Get(key []byte) (val []byte, ok bool) {
	path := l.pathToKey(key)
	node := l.getNode(path[0])
	if l.compareKeys(key, node.key) == 0 {
		val, ok = node.val, true
	}
	return
}

func (l *List) Put(key, val []byte) {
	if key == nil {
		panic("key must be non-nil")
	}
	if len(l.nodes) >= maxCount {
		panic("list has no capacity")
	}

	// find the path to the greatest
	// existing node key less than |key|
	path := l.pathBeforeKey(key)

	// check if |key| exists in |l|
	node := l.getNode(path[0])
	node = l.getNode(node.next[0])

	if l.compareKeys(key, node.key) == 0 {
		l.overwrite(key, val, path, node)
	} else {
		l.insert(key, val, path)
		l.count++
	}
}

func (l *List) pathToKey(key []byte) (path skipPointer) {
	next := l.headPointer()
	prev := sentinelId

	for lvl := int(maxHeight); lvl >= 0; {
		curr := l.getNode(next[lvl])

		// descend if we can't advance at |lvl|
		if l.compareKeys(key, curr.key) < 0 {
			path[lvl] = prev
			lvl--
			continue
		}

		// advance
		next = curr.next
		prev = curr.id
	}
	return
}

func (l *List) pathBeforeKey(key []byte) (path skipPointer) {
	next := l.headPointer()
	prev := sentinelId

	for lvl := int(maxHeight); lvl >= 0; {
		curr := l.getNode(next[lvl])

		// descend if we can't advance at |lvl|
		if l.compareKeys(key, curr.key) <= 0 {
			path[lvl] = prev
			lvl--
			continue
		}

		// advance
		next = curr.next
		prev = curr.id
	}
	return
}

func (l *List) insert(key, value []byte, path skipPointer) {
	novel := skipNode{
		key:    key,
		val:    value,
		id:     l.nextNodeId(),
		height: rollHeight(),
	}
	l.nodes = append(l.nodes, novel)

	for h := uint8(0); h <= novel.height; h++ {
		// set forward pointers
		n := l.getNode(path[h])
		novel.next[h] = n.next[h]
		n.next[h] = novel.id
		l.updateNode(n)
	}

	// set back pointers
	n := l.getNode(novel.next[0])
	novel.prev = n.prev
	l.updateNode(novel)
	n.prev = novel.id
	l.updateNode(n)
}

func (l *List) overwrite(key, value []byte, path skipPointer, old skipNode) {
	novel := old
	novel.id = l.nextNodeId()
	novel.key = key
	novel.val = value
	l.nodes = append(l.nodes, novel)

	for h := uint8(0); h <= novel.height; h++ {
		// set forward pointers
		n := l.getNode(path[h])
		n.next[h] = novel.id
		l.updateNode(n)
	}

	// set back pointer
	n := l.getNode(novel.next[0])
	n.prev = novel.id
	l.updateNode(n)
}

type ListIter struct {
	curr skipNode
	list *List
}

func (it *ListIter) Count() int {
	return it.list.Count()
}

func (it *ListIter) Current() (key, val []byte) {
	return it.curr.key, it.curr.val
}

func (it *ListIter) Advance() {
	it.curr = it.list.getNode(it.curr.next[0])
	return
}

func (it *ListIter) Retreat() {
	it.curr = it.list.getNode(it.curr.prev)
	return
}

func (l *List) GetIterAt(key []byte) (it *ListIter) {
	return l.GetIterFromSearchFn(func(nodeKey []byte) bool {
		return l.compareKeys(key, nodeKey) > 0
	})
}

func (l *List) GetIterFromSearchFn(fn SeekFn) (it *ListIter) {
	it = &ListIter{
		curr: l.seekWithFn(fn),
		list: l,
	}
	if it.curr.id == sentinelId {
		// try to keep |it| in bounds if |key| is
		// greater than the largest key in |l|
		it.Retreat()
	}
	return
}

func (l *List) IterAtStart() *ListIter {
	return &ListIter{
		curr: l.firstNode(),
		list: l,
	}
}

func (l *List) IterAtEnd() *ListIter {
	return &ListIter{
		curr: l.lastNode(),
		list: l,
	}
}

// seek returns the skipNode with the smallest key >= |key|.
func (l *List) seek(key []byte) skipNode {
	return l.seekWithFn(func(curr []byte) (advance bool) {
		return l.compareKeys(key, curr) > 0
	})
}

func (l *List) seekWithFn(cb SeekFn) (node skipNode) {
	ptr := l.headPointer()
	for h := int64(maxHeight); h >= 0; h-- {
		node = l.getNode(ptr[h])
		for cb(node.key) {
			ptr = node.next
			node = l.getNode(ptr[h])
		}
	}
	return
}

func (l *List) headPointer() skipPointer {
	return l.nodes[0].next
}

func (l *List) firstNode() skipNode {
	return l.getNode(l.nodes[0].next[0])
}

func (l *List) lastNode() skipNode {
	s := l.getNode(sentinelId)
	return l.getNode(s.prev)
}

func (l *List) getNode(id nodeId) skipNode {
	return l.nodes[id]
}

func (l *List) updateNode(node skipNode) {
	l.nodes[node.id] = node
}

func (l *List) nextNodeId() nodeId {
	return nodeId(len(l.nodes))
}

func (l *List) compareKeys(left, right []byte) int {
	if right == nil {
		return -1 // |right| is sentinel key
	}
	return l.keyOrder(left, right)
}

var (
	// Precompute the skiplist probabilities so that the optimal
	// p-value can be used (inverse of Euler's number).
	//
	// https://github.com/andy-kimball/arenaskl/blob/master/skl.go
	probabilities = [maxHeight]uint32{}
	randSrc       = rand.New(rand.NewSource(rand.Int63()))
)

func init() {
	p := float64(1.0)
	for i := uint8(0); i < maxHeight; i++ {
		p /= math.E
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
	}
}

func rollHeight() (h uint8) {
	rnd := randSrc.Uint32()
	h = 0
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}
	return
}
