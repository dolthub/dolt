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
	"math"
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
	ok, err := cur.Advance(ctx)
	if err != nil {
		return nil, err
	}
	if ok {
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

func (cur *Cursor) Invalidate() {
	cur.idx = math.MinInt32
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

func (cur *Cursor) seek(ctx context.Context, item Item, cb CompareFn) (err error) {
	inBounds := true
	if cur.parent != nil {
		inBounds = inBounds && cb(item, cur.firstKey()) >= 0
		inBounds = inBounds && cb(item, cur.lastKey()) <= 0
	}

	if !inBounds {
		// |item| is outside the bounds of |cur.nd|, search up the tree
		err = cur.parent.seek(ctx, item, cb)
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

	cur.idx = cur.search(item, cb)

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

// todo(andy): improve the combined interface of Advance() and advanceInBounds().
//  currently the returned boolean indicates if the cursor was able to Advance,
//  which isn't usually useful information

func (cur *Cursor) Advance(ctx context.Context) (bool, error) {
	ok, err := cur.advanceInBounds(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		cur.idx = int(cur.nd.count)
	}

	return ok, nil
}

func (cur *Cursor) advanceInBounds(ctx context.Context) (bool, error) {
	lastKeyIdx := int(cur.nd.count - 1)
	if cur.idx < lastKeyIdx {
		cur.idx += 1
		return true, nil
	}

	if cur.idx == int(cur.nd.count) {
		// |cur| is already out of bounds
		return false, nil
	}

	assertTrue(cur.atNodeEnd())

	if cur.parent != nil {
		ok, err := cur.parent.advanceInBounds(ctx)

		if err != nil {
			return false, err
		}

		if ok {
			// at end of currentPair chunk and there are more
			err := cur.fetchNode(ctx)
			if err != nil {
				return false, err
			}

			cur.skipToNodeStart()
			cur.subtrees = nil // lazy load

			return true, nil
		}
		// if not |ok|, then every parent, grandparent, etc.,
		// failed to advanceInBounds(): we're past the end
		// of the prolly tree.
	}

	return false, nil
}

func (cur *Cursor) Retreat(ctx context.Context) (bool, error) {
	ok, err := cur.retreatInBounds(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		cur.idx = -1
	}

	return ok, nil
}

func (cur *Cursor) retreatInBounds(ctx context.Context) (bool, error) {
	if cur.idx > 0 {
		cur.idx -= 1
		return true, nil
	}

	if cur.idx == -1 {
		// |cur| is already out of bounds
		return false, nil
	}

	assertTrue(cur.atNodeStart())

	if cur.parent != nil {
		ok, err := cur.parent.retreatInBounds(ctx)

		if err != nil {
			return false, err
		}

		if ok {
			err := cur.fetchNode(ctx)
			if err != nil {
				return false, err
			}

			cur.skipToNodeEnd()
			cur.subtrees = nil // lazy load

			return true, nil
		}
		// if not |ok|, then every parent, grandparent, etc.,
		// failed to retreatInBounds(): we're before the start.
		// of the prolly tree.
	}

	return false, nil
}

// fetchNode loads the Node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *Cursor) fetchNode(ctx context.Context) (err error) {
	assertTrue(cur.parent != nil)
	cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.CurrentRef())
	cur.idx = -1 // caller must set
	return err
}

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
