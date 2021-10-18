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

package prolly

import (
	"context"
	"sort"

	"github.com/dolthub/dolt/go/store/d"
)

type compareItems func(left, right nodeItem) int

type cursorFn func(cur *nodeCursor) error

// nodeCursor explores a tree of node items.
type nodeCursor struct {
	nd     node
	idx    int
	parent *nodeCursor
	nrw    NodeReadWriter
}

func newCursor(ctx context.Context, nrw NodeReadWriter, nd node) (cur *nodeCursor, err error) {
	cur = &nodeCursor{nd: nd, nrw: nrw}
	for !cur.nd.leafNode() {
		nd, err = fetchRef(ctx, nrw, cur.current())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &nodeCursor{nd: nd, parent: parent, nrw: nrw}
	}
	return
}

func newCursorAtItem(ctx context.Context, nrw NodeReadWriter, nd node, item nodeItem, cmp compareItems, cb cursorFn) (err error) {
	cur := nodeCursor{nd: nd, nrw: nrw}

	err = cur.seek(ctx, nrw, item, cmp)
	if err != nil {
		return err
	}

	for !cur.nd.leafNode() {
		nd, err = fetchRef(ctx, nrw, cur.current())
		if err != nil {
			return err
		}

		parent := cur
		cur = nodeCursor{nd: nd, parent: &parent, nrw: nrw}

		err = cur.seek(ctx, nrw, item, cmp)
		if err != nil {
			return err
		}
	}

	return cb(&cur)
}

func (cur *nodeCursor) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.nd.nodeCount()
}

// current returns the item at the current cursor position
func (cur *nodeCursor) current() nodeItem {
	d.PanicIfFalse(cur.valid())
	return cur.nd.getItem(cur.idx)
}

func (cur *nodeCursor) firstItem() nodeItem {
	return cur.nd.getItem(0)
}

func (cur *nodeCursor) lastItem() nodeItem {
	return cur.nd.getItem(cur.lastIdx())
}

func (cur *nodeCursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *nodeCursor) skipToNodeEnd() {
	cur.idx = cur.lastIdx()
}

func (cur *nodeCursor) atNodeStart() bool {
	return cur.idx == 0
}

func (cur *nodeCursor) atNodeEnd() bool {
	return cur.idx == cur.lastIdx()
}

func (cur *nodeCursor) lastIdx() int {
	return cur.nd.nodeCount() - 1
}

func (cur *nodeCursor) level() uint64 {
	return uint64(cur.nd.level())
}

func (cur *nodeCursor) seek(ctx context.Context, nrw NodeReadWriter, item nodeItem, cb compareItems) (err error) {
	inBounds := true
	if cur.parent != nil {
		inBounds = cb(item, cur.firstItem()) >= 0 || cb(item, cur.lastItem()) <= 0
	}

	if !inBounds {
		// |item| is outside the bounds of |cur.nd|, search up the tree
		err = cur.parent.seek(ctx, nrw, item, cb)
		if err != nil {
			return err
		}

		cur.nd, err = fetchRef(ctx, nrw, cur.parent.current())
		if err != nil {
			return err
		}
	}

	cur.idx = cur.search(item, cb)

	return
}

// search returns the index of |item| if it's present in |cur.nd|, or the
// index of the next greatest element if it is not present.
func (cur *nodeCursor) search(item nodeItem, cb compareItems) int {
	// todo(andy): this is specific to Map
	if cur.level() == 0 {
		// leaf nodes
		idx := sort.Search(cur.nd.nodeCount()/2, func(i int) bool {
			return cb(item, cur.nd.getItem(i*2)) <= 0
		})
		return idx * 2
	} else {
		// internal nodes
		return sort.Search(cur.nd.nodeCount(), func(i int) bool {
			return cb(item, cur.nd.getItem(i)) <= 0
		})
	}
}

func (cur *nodeCursor) validateNode(cb compareItems) {
	if cur.level() == 0 {
		for i := 2; i < cur.nd.nodeCount(); i += 2 {
			prev := cur.nd.getItem(i - 2)
			curr := cur.nd.getItem(i)
			if cb(prev, curr) != -1 {
				panic("")
			}
		}
	} else {
		for i := 1; i < cur.nd.nodeCount(); i++ {
			prev := cur.nd.getItem(i - 1)
			curr := cur.nd.getItem(i)
			if cb(prev, curr) != -1 {
				panic("")
			}
		}
	}
}

func (cur *nodeCursor) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur *nodeCursor) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
	if cur.idx < cur.nd.nodeCount()-1 {
		cur.idx++
		return true, nil
	}

	if cur.idx == cur.nd.nodeCount() {
		return false, nil
	}

	if cur.parent != nil {
		ok, err := cur.parent.advanceMaybeAllowPastEnd(ctx, false)

		if err != nil {
			return false, err
		}

		if ok {
			// at end of current leaf chunk and there are more
			err := cur.fetchNode(ctx)
			if err != nil {
				return false, err
			}

			cur.skipToNodeStart()
			return true, nil
		}
	}

	if allowPastEnd {
		cur.idx++
	}

	return false, nil
}

func (cur *nodeCursor) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur *nodeCursor) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
	if cur.idx > 0 {
		cur.idx--
		return true, nil
	}

	if cur.idx == -1 {
		return false, nil
	}

	d.PanicIfFalse(0 == cur.idx)

	if cur.parent != nil {
		ok, err := cur.parent.retreatMaybeAllowBeforeStart(ctx, false)

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
	}

	if allowBeforeStart {
		cur.idx--
	}

	return false, nil
}

// fetchNode loads the node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *nodeCursor) fetchNode(ctx context.Context) (err error) {
	d.PanicIfFalse(cur.parent != nil)
	cur.nd, err = fetchRef(ctx, cur.nrw, cur.parent.current())
	cur.idx = -1 // caller must set
	return err
}

func (cur *nodeCursor) compare(other *nodeCursor) int {
	if cur.parent != nil {
		p := cur.parent.compare(other.parent)
		if p != 0 {
			return p
		}
	}
	d.PanicIfFalse(cur.nd.nodeCount() == other.nd.nodeCount())
	return cur.idx - other.idx
}

func iterTree(ctx context.Context, nrw NodeReadWriter, nd node, cb func(item nodeItem) error) error {
	if nd.empty() {
		return nil
	}

	cur, err := newCursor(ctx, nrw, nd)
	if err != nil {
		return err
	}

	ok := true
	for ok {
		curr := cur.current()

		err = cb(curr)
		if err != nil {
			return err
		}

		ok, err = cur.advance(ctx)
		if err != nil {
			return err
		}
	}
	return err
}
