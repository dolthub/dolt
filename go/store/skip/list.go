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
	"context"
	"hash/maphash"
	"math"
)

const (
	maxHeight  = 9
	maxCount   = math.MaxInt32 - 1
	sentinelId = nodeId(0)
	initSize   = 8
)

// A KeyOrder determines the ordering of two keys |l| and |r|.
type KeyOrder func(ctx context.Context, l, r []byte) (cmp int)

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
func (l *List) Revert(ctx context.Context) {
	cp := l.checkpoint
	keepers := l.nodes[1:cp]
	l.Truncate()
	for _, nd := range keepers {
		l.Put(ctx, nd.key, nd.val)
	}
	l.checkpoint = cp
}

// Truncate deletes all entries from the list.
func (l *List) Truncate() {
	l.nodes = l.nodes[:1]
	// point sentinel.prev at itself
	s := l.nodePtr(sentinelId)
	s.next = tower{}
	s.prev = sentinelId
	l.checkpoint = nodeId(1)
	l.count = 0
}

// Count returns the number of items in the list.
func (l *List) Count() int {
	return int(l.count)
}

// Has returns true if |key| is a member of the list.
func (l *List) Has(ctx context.Context, key []byte) (ok bool) {
	_, ok = l.Get(ctx, key)
	return
}

// Get returns the value associated with |key| and true
// if |key| is a member of the list, otherwise it returns
// nil and false.
func (l *List) Get(ctx context.Context, key []byte) (val []byte, ok bool) {
	var id nodeId
	next, prev := l.headTower(), sentinelId
	for lvl := maxHeight; lvl >= 0; {
		nd := l.nodePtr(next[lvl])
		// descend if we can't advance at |lvl|
		if l.compareKeys(ctx, key, nd.key) < 0 {
			id = prev
			lvl--
			continue
		}
		// advance
		next = &nd.next
		prev = nd.id
	}
	node := l.nodePtr(id)
	if l.compareKeys(ctx, key, node.key) == 0 {
		val, ok = node.val, true
	}
	return
}

// Put adds |key| and |values| to the list.
func (l *List) Put(ctx context.Context, key, val []byte) {
	if key == nil {
		panic("key must be non-nil")
	} else if len(l.nodes) >= maxCount {
		panic("list has no capacity")
	}

	// find the path to the greatest
	// existing node key less than |key|
	var path tower
	next, prev := l.headTower(), sentinelId
	for h := maxHeight; h >= 0; {
		curr := l.nodePtr(next[h])
		// descend if we can't advance at |lvl|
		if l.compareKeys(ctx, key, curr.key) <= 0 {
			path[h] = prev
			h--
			continue
		}
		// advance
		next = &curr.next
		prev = curr.id
	}

	// check if |key| exists in |l|
	node := l.nodePtr(path[0])
	node = l.nodePtr(node.next[0])

	if l.compareKeys(ctx, key, node.key) == 0 {
		l.overwrite(key, val, &path, node)
	} else {
		l.insert(key, val, &path)
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

func (l *List) insert(key, value []byte, path *tower) {
	id := l.nextNodeId()
	l.nodes = append(l.nodes, skipNode{
		key:    key,
		val:    value,
		id:     id,
		height: l.rollHeight(key),
	})
	novel := l.nodePtr(id)
	for h := uint8(0); h <= novel.height; h++ {
		// set forward pointers
		n := l.nodePtr(path[h])
		novel.next[h] = n.next[h]
		n.next[h] = novel.id
	}
	// set back pointers
	n := l.nodePtr(novel.next[0])
	novel.prev = n.prev
	n.prev = novel.id
}

func (l *List) overwrite(key, value []byte, path *tower, old *skipNode) {
	id := l.nextNodeId()
	l.nodes = append(l.nodes, skipNode{
		key:    key,
		val:    value,
		id:     id,
		next:   old.next,
		prev:   old.prev,
		height: old.height,
	})
	for h := uint8(0); h <= old.height; h++ {
		// set forward pointers
		n := l.nodePtr(path[h])
		n.next[h] = id
	}
	// set back pointer
	n := l.nodePtr(old.next[0])
	n.prev = id
}

type ListIter struct {
	curr *skipNode
	list *List
}

// Current returns the current key and value of the iterator.
func (it *ListIter) Current() (key, val []byte) {
	return it.curr.key, it.curr.val
}

// Advance advances the iterator.
func (it *ListIter) Advance() {
	it.curr = it.list.nodePtr(it.curr.next[0])
	return
}

// Retreat retreats the iterator.
func (it *ListIter) Retreat() {
	it.curr = it.list.nodePtr(it.curr.prev)
	return
}

// GetIterAt creates an iterator starting at the first item
// of the list whose key is greater than or equal to |key|.
func (l *List) GetIterAt(ctx context.Context, key []byte) (it *ListIter) {
	return l.GetIterFromSeekFn(func(nodeKey []byte) bool {
		return l.compareKeys(ctx, key, nodeKey) > 0
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
func (l *List) seek(ctx context.Context, key []byte) *skipNode {
	return l.seekWithFn(func(curr []byte) (advance bool) {
		return l.compareKeys(ctx, key, curr) > 0
	})
}

func (l *List) seekWithFn(cb SeekFn) (node *skipNode) {
	ptr := l.headTower()
	for h := int64(maxHeight); h >= 0; h-- {
		node = l.nodePtr(ptr[h])
		for cb(node.key) {
			ptr = &node.next
			node = l.nodePtr(ptr[h])
		}
	}
	return
}

func (l *List) headTower() *tower {
	return &l.nodes[0].next
}

func (l *List) firstNode() *skipNode {
	return l.nodePtr(l.nodes[0].next[0])
}

func (l *List) lastNode() *skipNode {
	s := l.nodePtr(sentinelId)
	return l.nodePtr(s.prev)
}

func (l *List) nodePtr(id nodeId) *skipNode {
	return &l.nodes[id]
}

func (l *List) nextNodeId() nodeId {
	return nodeId(len(l.nodes))
}

func (l *List) compareKeys(ctx context.Context, left, right []byte) int {
	if right == nil {
		return -1 // |right| is sentinel key
	}
	return l.keyOrder(ctx, left, right)
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
