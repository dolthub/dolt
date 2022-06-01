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

package tree

import (
	"context"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
)

// Cursor explores a tree of Nodes.
type Cursor struct {
	nd       Node
	idx      int
	parent   *Cursor
	subtrees SubtreeCounts
	nrw      NodeStore
}

type SubtreeCounts []uint64

func (sc SubtreeCounts) Sum() (s uint64) {
	for _, count := range sc {
		s += count
	}
	return
}

type CompareFn func(left, right Item) int

type SearchFn func(nd Node) (idx int)

type ItemSearchFn func(item Item, nd Node) (idx int)

func NewCursorAtStart(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}
	for !cur.isLeaf() {
		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}
	}
	return
}

func NewCursorAtEnd(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}
	cur.skipToNodeEnd()

	for !cur.isLeaf() {
		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}
		cur.skipToNodeEnd()
	}
	return
}

func NewCursorPastEnd(ctx context.Context, ns NodeStore, nd Node) (cur *Cursor, err error) {
	cur, err = NewCursorAtEnd(ctx, ns, nd)
	if err != nil {
		return nil, err
	}

	// Advance |cur| past the end
	err = cur.Advance(ctx)
	if err != nil {
		return nil, err
	}
	if cur.idx != int(cur.nd.count) {
		panic("expected |ok| to be  false")
	}

	return
}

func NewCursorAtOrdinal(ctx context.Context, ns NodeStore, nd Node, ord uint64) (cur *Cursor, err error) {
	if ord >= uint64(nd.TreeCount()) {
		return NewCursorPastEnd(ctx, ns, nd)
	}

	distance := int64(ord)
	return NewCursorFromSearchFn(ctx, ns, nd, func(nd Node) (idx int) {
		if nd.IsLeaf() {
			return int(distance)
		}

		// |subtrees| contains cardinalities of each child tree in |nd|
		subtrees := nd.getSubtreeCounts()

		for idx = range subtrees {
			card := int64(subtrees[idx])
			if (distance - card) < 0 {
				break
			}
			distance -= card
		}
		return
	})
}

func NewCursorFromSearchFn(ctx context.Context, ns NodeStore, nd Node, search SearchFn) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}

	cur.idx = search(cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}

		cur.idx = search(cur.nd)
	}

	return
}

func NewCursorFromCompareFn(ctx context.Context, ns NodeStore, n Node, i Item, compare CompareFn) (*Cursor, error) {
	return NewCursorAtItem(ctx, ns, n, i, func(item Item, node Node) (idx int) {
		return sort.Search(node.Count(), func(i int) bool {
			return compare(item, node.GetKey(i)) <= 0
		})
	})
}

func NewCursorAtItem(ctx context.Context, ns NodeStore, nd Node, item Item, search ItemSearchFn) (cur *Cursor, err error) {
	cur = &Cursor{nd: nd, nrw: ns}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, nrw: ns}

		cur.idx = search(item, cur.nd)
	}

	return
}

func NewLeafCursorAtItem(ctx context.Context, ns NodeStore, nd Node, item Item, search ItemSearchFn) (cur Cursor, err error) {
	cur = Cursor{nd: nd, parent: nil, nrw: ns}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		// reuse |cur| object to keep stack alloc'd
		cur.nd, err = fetchChild(ctx, ns, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
}

func CurrentCursorItems(cur *Cursor) (key, value Item) {
	key = cur.nd.keys.GetSlice(cur.idx)
	value = cur.nd.values.GetSlice(cur.idx)
	return
}

func (cur *Cursor) Valid() bool {
	return cur.nd.count != 0 &&
		cur.nd.bytes() != nil &&
		cur.idx >= 0 &&
		cur.idx < int(cur.nd.count)
}

func (cur *Cursor) CurrentKey() Item {
	return cur.nd.GetKey(cur.idx)
}

func (cur *Cursor) CurrentValue() Item {
	return cur.nd.getValue(cur.idx)
}

func (cur *Cursor) CurrentRef() hash.Hash {
	return cur.nd.getAddress(cur.idx)
}

func (cur *Cursor) currentSubtreeSize() uint64 {
	if cur.isLeaf() {
		return 1
	}
	if cur.subtrees == nil { // lazy load
		cur.subtrees = cur.nd.getSubtreeCounts()
	}
	return cur.subtrees[cur.idx]
}

func (cur *Cursor) firstKey() Item {
	return cur.nd.GetKey(0)
}

func (cur *Cursor) lastKey() Item {
	lastKeyIdx := int(cur.nd.count - 1)
	return cur.nd.GetKey(lastKeyIdx)
}

func (cur *Cursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *Cursor) skipToNodeEnd() {
	lastKeyIdx := int(cur.nd.count - 1)
	cur.idx = lastKeyIdx
}

func (cur *Cursor) keepInBounds() {
	if cur.idx < 0 {
		cur.skipToNodeStart()
	}
	lastKeyIdx := int(cur.nd.count - 1)
	if cur.idx > lastKeyIdx {
		cur.skipToNodeEnd()
	}
}

func (cur *Cursor) atNodeStart() bool {
	return cur.idx == 0
}

// atNodeEnd returns true if the cursor's current |idx|
// points to the last node item
func (cur *Cursor) atNodeEnd() bool {
	lastKeyIdx := int(cur.nd.count - 1)
	return cur.idx == lastKeyIdx
}

func (cur *Cursor) isLeaf() bool {
	// todo(andy): cache Level
	return cur.level() == 0
}

func (cur *Cursor) level() uint64 {
	return uint64(cur.nd.Level())
}

// seek updates the cursor's node to one whose range spans the key's value, or the last
// node if the key is greater than all existing keys.
// If a node does not contain the key, we recurse upwards to the parent cursor. If the
// node contains a key, we recurse downwards into child nodes.
func (cur *Cursor) seek(ctx context.Context, key Item, cb CompareFn) (err error) {
	inBounds := true
	if cur.parent != nil {
		inBounds = inBounds && cb(key, cur.firstKey()) >= 0
		inBounds = inBounds && cb(key, cur.lastKey()) <= 0
	}

	if !inBounds {
		// |item| is outside the bounds of |cur.nd|, search up the tree
		err = cur.parent.seek(ctx, key, cb)
		if err != nil {
			return err
		}
		// stay in bounds for internal nodes
		cur.parent.keepInBounds()

		cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.CurrentRef())
		if err != nil {
			return err
		}
	}

	cur.idx = cur.search(key, cb)

	return
}

// search returns the index of |item| if it's present in |cur.nd|, or the
// index of the next greatest element if it's not present.
func (cur *Cursor) search(item Item, cb CompareFn) (idx int) {
	idx = sort.Search(int(cur.nd.count), func(i int) bool {
		return cb(item, cur.nd.GetKey(i)) <= 0
	})

	return idx
}

// invalidate sets the cursor's index to the node count.
func (cur *Cursor) invalidate() {
	cur.idx = int(cur.nd.count)
}

// hasNext returns true if we do not need to recursively
// check the parent to know that the current cursor
// has more keys. hasNext can be false even if parent
// cursors are not exhausted.
func (cur *Cursor) hasNext() bool {
	return cur.idx < int(cur.nd.count)-1
}

// hasPrev returns true if the current node has preceding
// keys. hasPrev can be false even in a parent node has
// preceding keys.
func (cur *Cursor) hasPrev() bool {
	return cur.idx > 0
}

// outOfBounds returns true if the current cursor and
// all parents are exhausted.
func (cur *Cursor) outOfBounds() bool {
	return cur.idx < 0 || cur.idx >= int(cur.nd.count)
}

// Advance either increments the current key index by one,
// or has reached the end of the current node and skips to the next
// child of the parent cursor, recursively if necessary, returning
// either an error or nil.
//
// More specifically, one of three things happens:
//
// 1) The current chunk still has keys, iterate to
// the next |idx|;
//
// 2) We've exhausted the current cursor, but there is at least
// one |parent| cursor with more keys. We find that |parent| recursively,
// perform step (1), and then have every child initialize itself
// using the new |parent|.
//
// 3) We've exhausted the current cursor and every |parent|. Jump
// to an end state (idx = node.count).
func (cur *Cursor) Advance(ctx context.Context) error {
	if cur.hasNext() {
		cur.idx++
		return nil
	}

	if cur.parent == nil {
		cur.invalidate()
		return nil
	}

	// recursively increment the parent
	err := cur.parent.Advance(ctx)
	if err != nil {
		return err
	}

	if cur.parent.outOfBounds() {
		// exhausted every parent cursor
		cur.invalidate()
		return nil
	}

	// new parent cursor points to new cur node
	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeStart()
	cur.subtrees = nil // lazy load

	return nil
}

// Retreat decrements to the previous key, if necessary by
// recursively decrementing parent nodes.
func (cur *Cursor) Retreat(ctx context.Context) error {
	if cur.hasPrev() {
		cur.idx--
		return nil
	}

	if cur.parent == nil {
		cur.invalidate()
		return nil
	}

	// recursively decrement the parent
	err := cur.parent.Retreat(ctx)
	if err != nil {
		return err
	}

	if cur.parent.outOfBounds() {
		// exhausted every parent cursor
		cur.invalidate()
		return nil
	}

	// new parent cursor points to new cur node
	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeEnd()
	cur.subtrees = nil // lazy load

	return nil
}

// fetchNode loads the Node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *Cursor) fetchNode(ctx context.Context) (err error) {
	assertTrue(cur.parent != nil)
	cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.CurrentRef())
	cur.idx = -1 // caller must set
	return err
}

// Compare returns the highest relative index difference
// between two cursor trees. A parent has a higher precedence
// than its child.
//
// Ex:
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 4
//    res => -2 (from level 0)
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 3, L1 -> 5, L0 -> 4
//    res => +1 (from level 2)
func (cur *Cursor) Compare(other *Cursor) int {
	return compareCursors(cur, other)
}

func (cur *Cursor) Clone() *Cursor {
	cln := Cursor{
		nd:  cur.nd,
		idx: cur.idx,
		nrw: cur.nrw,
	}

	if cur.parent != nil {
		cln.parent = cur.parent.Clone()
	}

	return &cln
}

func (cur *Cursor) copy(other *Cursor) {
	cur.nd = other.nd
	cur.idx = other.idx
	cur.nrw = other.nrw

	if cur.parent != nil {
		assertTrue(other.parent != nil)
		cur.parent.copy(other.parent)
	} else {
		assertTrue(other.parent == nil)
	}
}

func compareCursors(left, right *Cursor) (diff int) {
	diff = 0
	for {
		d := left.idx - right.idx
		if d != 0 {
			diff = d
		}

		if left.parent == nil || right.parent == nil {
			break
		}
		left, right = left.parent, right.parent
	}
	return
}

func fetchChild(ctx context.Context, ns NodeStore, ref hash.Hash) (Node, error) {
	// todo(andy) handle nil Node, dangling ref
	return ns.Read(ctx, ref)
}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
