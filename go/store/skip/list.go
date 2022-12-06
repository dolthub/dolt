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
	"hash/maphash"
	"math"
)

const (
	maxHeight  = 9
	maxCount   = math.MaxUint32 - 1
	sentinelId = nodeId(0)
	initSize   = 8
)

// A KeyOrder determines the ordering of two keys |l| and |r|.
type KeyOrder func(l, r []byte) (cmp int)

// A SeekFn facilitates seeking into a List. It returns true
// if the seek operation should advance past |key|.
type SeekFn func(key []byte) (advance bool)

// List is an in-memory skip-list.
type List struct {
	// nodes contains all skipNode's in the List.
	// skipNode's are assigned ascending id's and
	// are stored in the order they were created,
	// i.e. skipNode.id stores its index in |nodes|
	nodes []skipNode

	// count stores the current number of items in
	// the list (updates are not made in-place)
	count uint32

	// checkpoint stores the nodeId of the last
	// checkpoint made. All nodes created after this
	// point will be discarded on a Revert()
	checkpoint nodeId

	// keyOrder determines the ordering of items
	keyOrder KeyOrder

	// seed is hash salt
	seed maphash.Seed
}

type nodeId uint32

// tower is a multi-level skipNode pointer.
type tower [maxHeight + 1]nodeId

type skipNode struct {
	key, val []byte
	id       nodeId
	next     tower
	prev     nodeId
	height   uint8
}

// NewSkipList returns a new skip.List.
func NewSkipList(order KeyOrder) *List {
	nodes := make([]skipNode, 0, initSize)

	// initialize sentinel node
	nodes = append(nodes, skipNode{
		id:     sentinelId,
		height: maxHeight,
		prev:   sentinelId,
	})

	return &List{
		nodes:      nodes,
		checkpoint: nodeId(1),
		keyOrder:   order,
		seed:       maphash.MakeSeed(),
	}
}

// Checkpoint records a checkpoint that can be reverted to.
func (l *List) Checkpoint() {
	l.checkpoint = l.nextNodeId()
}

func (l *List) HasCheckpoint() bool {
	return l.checkpoint > nodeId(1)
}

// Revert reverts to the last recorded checkpoint.
func (l *List) Revert() {
	cp := l.checkpoint
	keepers := l.nodes[1:cp]
	l.Truncate()
	for _, nd := range keepers {
		l.Put(nd.key, nd.val)
	}
	l.checkpoint = cp
}

// Truncate deletes all entries from the list.
func (l *List) Truncate() {
	l.nodes = l.nodes[:1]
	// point sentinel.prev at itself
	s := l.getNode(sentinelId)
	s.next = tower{}
	s.prev = sentinelId
	l.updateNode(s)
	l.checkpoint = nodeId(1)
	l.count = 0
}

// Count returns the number of items in the list.
func (l *List) Count() int {
	return int(l.count)
}

// Has returns true if |key| is a member of the list.
func (l *List) Has(key []byte) (ok bool) {
	_, ok = l.Get(key)
	return
}

// Get returns the value associated with |key| and true
// if |key| is a member of the list, otherwise it returns
// nil and false.
func (l *List) Get(key []byte) (val []byte, ok bool) {
	path := l.pathToKey(key)
	node := l.getNode(path[0])
	if l.compareKeys(key, node.key) == 0 {
		val, ok = node.val, true
	}
	return
}

// Put adds |key| and |values| to the list.
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

func (l *List) Copy() *List {
	copies := make([]skipNode, len(l.nodes))
	copy(copies, l.nodes)
	return &List{
		nodes:      copies,
		count:      l.count,
		checkpoint: l.checkpoint,
		keyOrder:   l.keyOrder,
		seed:       l.seed,
	}
}

func (l *List) pathToKey(key []byte) (path tower) {
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

func (l *List) pathBeforeKey(key []byte) (path tower) {
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

func (l *List) insert(key, value []byte, path tower) {
	novel := skipNode{
		key:    key,
		val:    value,
		id:     l.nextNodeId(),
		height: l.rollHeight(key),
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

func (l *List) overwrite(key, value []byte, path tower, old skipNode) {
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

// Current returns the current key and value of the iterator.
func (it *ListIter) Current() (key, val []byte) {
	return it.curr.key, it.curr.val
}

// Advance advances the iterator.
func (it *ListIter) Advance() {
	it.curr = it.list.getNode(it.curr.next[0])
	return
}

// Retreat retreats the iterator.
func (it *ListIter) Retreat() {
	it.curr = it.list.getNode(it.curr.prev)
	return
}

// GetIterAt creates an iterator starting at the first item
// of the list whose key is greater than or equal to |key|.
func (l *List) GetIterAt(key []byte) (it *ListIter) {
	return l.GetIterFromSeekFn(func(nodeKey []byte) bool {
		return l.compareKeys(key, nodeKey) > 0
	})
}

// GetIterFromSeekFn creates an iterator using a SeekFn.
func (l *List) GetIterFromSeekFn(fn SeekFn) (it *ListIter) {
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

// IterAtStart creates an iterator at the start of the list.
func (l *List) IterAtStart() *ListIter {
	return &ListIter{
		curr: l.firstNode(),
		list: l,
	}
}

// IterAtEnd creates an iterator at the end of the list.
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

func (l *List) headPointer() tower {
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
)

func init() {
	p := float64(1.0)
	for i := uint8(0); i < maxHeight; i++ {
		p /= math.E
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
	}
}

func (l *List) rollHeight(key []byte) (h uint8) {
	rnd := maphash.Bytes(l.seed, key)
	for h < maxHeight && uint32(rnd) <= probabilities[h] {
		h++
	}
	return
}
