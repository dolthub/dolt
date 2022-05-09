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
	maxCount = math.MaxUint32 - 1

	maxHeight = uint8(5)
	highest   = maxHeight - 1

	sentinelId = nodeId(0)
)

type List struct {
	head  skipPointer
	nodes []skipNode
	count uint32

	checkpoint nodeId

	cmp ValueCmp
	src rand.Source
}

type ValueCmp func(left, right []byte) int

type SearchFn func(nodeKey []byte) bool

type nodeId uint32

type skipPointer [maxHeight]nodeId

type skipNode struct {
	key, val []byte

	id     nodeId
	next   skipPointer
	prev   nodeId
	height uint8
}

func NewSkipList(cmp ValueCmp) (l *List) {
	l = &List{
		// point |head| at sentinel
		head: skipPointer{},
		// todo(andy): buffer pool
		nodes: make([]skipNode, 1, 128),
		cmp:   cmp,
		src:   rand.NewSource(0),
	}

	// initialize sentinel node
	l.nodes[sentinelId] = skipNode{
		id:  sentinelId,
		key: nil, val: nil,
		height: maxHeight,
		next:   skipPointer{},
		prev:   sentinelId,
	}

	return
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
	// point |head| at sentinel
	l.head = skipPointer{}
	// point sentinel.prev at itself
	s := l.getNode(sentinelId)
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
	if l.Count() >= maxCount {
		panic("list has no capacity")
	}

	path := l.pathToKey(key)
	node := l.getNode(path[0])

	if l.compareKeys(key, node.key) == 0 {
		l.overwrite(key, val, node)
	} else {
		l.insert(key, val, path)
		l.count++
	}
}

func (l *List) pathToKey(key []byte) (path skipPointer) {
	next := l.head
	prev := sentinelId

	for lvl := int(highest); lvl >= 0; {
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
	next := l.head
	prev := sentinelId

	for lvl := int(highest); lvl >= 0; {
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

func (l *List) insert(key, value []byte, history skipPointer) {
	nd := skipNode{
		id:     nodeId(len(l.nodes)),
		key:    key,
		val:    value,
		height: rollHeight(l.src),
		next:   skipPointer{},
		prev:   sentinelId,
	}
	l.nodes = append(l.nodes, nd)

	for h := uint8(0); h <= nd.height; h++ {
		// if |node.key| is the smallest key for
		// level |h| then update |l.head|
		first := l.getNode(l.head[h])
		if l.compare(nd, first) < 0 {
			l.head[h] = nd.id
			nd.next[h] = first.id
			continue
		}

		// otherwise, splice in |node| using |history|
		prevNd := l.getNode(history[h])
		nd.next[h] = prevNd.next[h]
		prevNd.next[h] = nd.id
		l.updateNode(prevNd)
	}

	// set back pointers for level 0
	nextNd := l.getNode(nd.next[0])
	nd.prev = nextNd.prev
	nextNd.prev = nd.id
	l.updateNode(nextNd)
	l.updateNode(nd)
}

func (l *List) overwrite(key, value []byte, old skipNode) {
	novel := old
	novel.id = l.nextNodeId()
	novel.key = key
	novel.val = value
	l.nodes = append(l.nodes, novel)

	// brute force update
	for i := range l.head {
		if l.head[i] == old.id {
			l.head[i] = novel.id
		}
	}
	for _, nd := range l.nodes {
		for i := range nd.next {
			if nd.next[i] == old.id {
				nd.next[i] = novel.id
			}
		}
		if nd.prev == old.id {
			nd.prev = novel.id
		}
		l.updateNode(nd)
	}
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
		return l.compareKeysWithFn(key, nodeKey, l.cmp) > 0
	})
}

func (l *List) GetIterFromSearchFn(kontinue SearchFn) (it *ListIter) {
	it = &ListIter{
		curr: l.seekWithSearchFn(kontinue),
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
	return l.seekWithCompare(key, l.cmp)
}

func (l *List) seekWithCompare(key []byte, cmp ValueCmp) (node skipNode) {
	return l.seekWithSearchFn(func(nodeKey []byte) bool {
		return l.compareKeysWithFn(key, nodeKey, cmp) > 0
	})
}

func (l *List) seekWithSearchFn(kontinue SearchFn) (node skipNode) {
	ptr := l.head
	for h := int64(highest); h >= 0; h-- {
		node = l.getNode(ptr[h])
		for kontinue(node.key) {
			ptr = node.next
			node = l.getNode(ptr[h])
		}
	}
	return
}

func (l *List) firstNode() skipNode {
	return l.getNode(l.head[0])
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

func (l *List) compare(left, right skipNode) int {
	return l.compareKeys(left.key, right.key)
}

func (l *List) compareKeys(left, right []byte) int {
	return l.compareKeysWithFn(left, right, l.cmp)
}

func (l *List) compareKeysWithFn(left, right []byte, cmp ValueCmp) int {
	if right == nil {
		return -1 // |right| is sentinel key
	}
	return cmp(left, right)
}

const (
	pattern0 = uint64(1<<3 - 1)
	pattern1 = uint64(1<<6 - 1)
	pattern2 = uint64(1<<9 - 1)
	pattern3 = uint64(1<<12 - 1)
)

func rollHeight(r rand.Source) (h uint8) {
	roll := r.Int63()
	patterns := []uint64{
		pattern0,
		pattern1,
		pattern2,
		pattern3,
	}

	for _, pat := range patterns {
		if uint64(roll)&pat != pat {
			break
		}
		h++
	}

	return
}

func assertTrue(b bool) {
	if !b {
		panic("expected true")
	}
}
