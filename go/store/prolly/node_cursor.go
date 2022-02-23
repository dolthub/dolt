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
	"math"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

type nodeItem []byte

func (i nodeItem) size() val.ByteSize {
	return val.ByteSize(len(i))
}

// nodeCursor explores a tree of Node items.
type nodeCursor struct {
	nd     Node
	idx    int
	parent *nodeCursor
	nrw    NodeStore
}

type compareFn func(left, right nodeItem) int

type searchFn func(item nodeItem, nd Node) (idx int)

func newCursorAtStart(ctx context.Context, nrw NodeStore, nd Node) (cur *nodeCursor, err error) {
	cur = &nodeCursor{nd: nd, nrw: nrw}
	for !cur.isLeaf() {
		nd, err = fetchChild(ctx, nrw, cur.currentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &nodeCursor{nd: nd, parent: parent, nrw: nrw}
	}
	return
}

func newCursorPastEnd(ctx context.Context, nrw NodeStore, nd Node) (cur *nodeCursor, err error) {
	cur = &nodeCursor{nd: nd, nrw: nrw}
	cur.skipToNodeEnd()

	for !cur.isLeaf() {
		nd, err = fetchChild(ctx, nrw, cur.currentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &nodeCursor{nd: nd, parent: parent, nrw: nrw}
		cur.skipToNodeEnd()
	}

	// advance |cur| past the end
	ok, err := cur.advance(ctx)
	if err != nil {
		return nil, err
	}
	if ok {
		panic("expected |ok| to be  false")
	}

	return
}

func newCursorAtTuple(ctx context.Context, nrw NodeStore, nd Node, tup val.Tuple, search searchFn) (cur *nodeCursor, err error) {
	return newCursorAtItem(ctx, nrw, nd, nodeItem(tup), search)
}

func newCursorAtItem(ctx context.Context, nrw NodeStore, nd Node, item nodeItem, search searchFn) (cur *nodeCursor, err error) {
	cur = &nodeCursor{nd: nd, nrw: nrw}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err = fetchChild(ctx, nrw, cur.currentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &nodeCursor{nd: nd, parent: parent, nrw: nrw}

		cur.idx = search(item, cur.nd)
	}

	return
}

func newLeafCursorAtItem(ctx context.Context, nrw NodeStore, nd Node, item nodeItem, search searchFn) (cur nodeCursor, err error) {
	cur = nodeCursor{nd: nd, parent: nil, nrw: nrw}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		// reuse |cur| object to keep stack alloc'd
		cur.nd, err = fetchChild(ctx, nrw, cur.currentRef())
		if err != nil {
			return cur, err
		}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
}

func (cur *nodeCursor) valid() bool {
	return cur.nd.count != 0 &&
		cur.nd.bytes() != nil &&
		cur.idx >= 0 &&
		cur.idx < int(cur.nd.count)
}

func (cur *nodeCursor) invalidate() {
	cur.idx = math.MinInt32
}

func (cur *nodeCursor) currentKey() nodeItem {
	return cur.nd.getKey(cur.idx)
}

func (cur *nodeCursor) currentValue() nodeItem {
	return cur.nd.getValue(cur.idx)
}

func (cur *nodeCursor) currentRef() hash.Hash {
	return cur.nd.getRef(cur.idx)
}

func (cur *nodeCursor) firstKey() nodeItem {
	return cur.nd.getKey(0)
}

func (cur *nodeCursor) lastKey() nodeItem {
	lastKeyIdx := int(cur.nd.count - 1)
	return cur.nd.getKey(lastKeyIdx)
}

func (cur *nodeCursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *nodeCursor) skipToNodeEnd() {
	lastKeyIdx := int(cur.nd.count - 1)
	cur.idx = lastKeyIdx
}

func (cur *nodeCursor) keepInBounds() {
	if cur.idx < 0 {
		cur.skipToNodeStart()
	}
	lastKeyIdx := int(cur.nd.count - 1)
	if cur.idx > lastKeyIdx {
		cur.skipToNodeEnd()
	}
}

func (cur *nodeCursor) atNodeStart() bool {
	return cur.idx == 0
}

func (cur *nodeCursor) atNodeEnd() bool {
	lastKeyIdx := int(cur.nd.count - 1)
	return cur.idx == lastKeyIdx
}

func (cur *nodeCursor) isLeaf() bool {
	return cur.level() == 0
}

func (cur *nodeCursor) level() uint64 {
	return uint64(cur.nd.level())
}

func (cur *nodeCursor) seek(ctx context.Context, item nodeItem, cb compareFn) (err error) {
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

		cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.currentRef())
		if err != nil {
			return err
		}
	}

	cur.idx = cur.search(item, cb)

	return
}

// search returns the index of |item| if it's present in |cur.nd|, or the
// index of the nextMutation greatest element if it is not present.
func (cur *nodeCursor) search(item nodeItem, cb compareFn) (idx int) {
	idx = sort.Search(int(cur.nd.count), func(i int) bool {
		return cb(item, cur.nd.getKey(i)) <= 0
	})

	return idx
}

func (cur *nodeCursor) advance(ctx context.Context) (bool, error) {
	ok, err := cur.advanceInBounds(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		cur.idx = int(cur.nd.count)
	}

	return ok, nil
}

func (cur *nodeCursor) advanceInBounds(ctx context.Context) (bool, error) {
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
			return true, nil
		}
		// if not |ok|, then every parent, grandparent, etc.,
		// failed to advanceInBounds(): we're past the end
		// of the prolly tree.
	}

	return false, nil
}

func (cur *nodeCursor) retreat(ctx context.Context) (bool, error) {
	ok, err := cur.retreatInBounds(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		cur.idx = -1
	}

	return ok, nil
}

func (cur *nodeCursor) retreatInBounds(ctx context.Context) (bool, error) {
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
func (cur *nodeCursor) fetchNode(ctx context.Context) (err error) {
	assertTrue(cur.parent != nil)
	cur.nd, err = fetchChild(ctx, cur.nrw, cur.parent.currentRef())
	cur.idx = -1 // caller must set
	return err
}

func (cur *nodeCursor) compare(other *nodeCursor) int {
	return compareCursors(cur, other)
}

func (cur *nodeCursor) clone() *nodeCursor {
	cln := nodeCursor{
		nd:  cur.nd,
		idx: cur.idx,
		nrw: cur.nrw,
	}

	if cur.parent != nil {
		cln.parent = cur.parent.clone()
	}

	return &cln
}

func (cur *nodeCursor) copy(other *nodeCursor) {
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

func compareCursors(left, right *nodeCursor) (diff int) {
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

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
