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
	"sync"

	"github.com/dolthub/dolt/go/store/d"
)

const tip = 0

type treeCursor []nodeCursor2

type nodeCursor2 struct {
	nd  Node
	idx int
	nrw NodeReadWriter
}

type treeCursorFn func(cur treeCursor) error

func newTreeCursor(ctx context.Context, nrw NodeReadWriter, nd Node) (cur treeCursor, err error) {
	cur = make(treeCursor, nd.level()+1)

	lvl := nd.level()
	cur[lvl] = nodeCursor2{nd: nd, nrw: nrw}
	lvl--

	for lvl >= 0 {
		nd, err = fetchRef(ctx, nrw, cur[lvl:].parent().current())
		if err != nil {
			return nil, err
		}

		cur[lvl] = nodeCursor2{nd: nd, nrw: nrw}
		lvl--
	}

	return
}

var curPool = sync.Pool{
	New: func() interface{} {
		return make([]nodeCursor2, 64)[:]
	},
}

func newTreeCursorAtItem(ctx context.Context, nrw NodeReadWriter, nd Node, item nodeItem, cmp compareItems, cb treeCursorFn) (err error) {
	arr := curPool.Get().([]nodeCursor2)
	defer func() { curPool.Put(arr) }()

	lvl := nd.level()
	cur := treeCursor(arr[:lvl+1])

	cur[lvl] = nodeCursor2{nd: nd, nrw: nrw}
	lvl--

	err = cur[lvl:].seek(ctx, nrw, item, cmp)
	if err != nil {
		return err
	}

	for lvl >= 0 {
		nd, err = fetchRef(ctx, nrw, cur[lvl:].parent().current())
		if err != nil {
			return err
		}

		cur[lvl] = nodeCursor2{nd: nd, nrw: nrw}
		lvl--

		err = cur[lvl:].seek(ctx, nrw, item, cmp)
		if err != nil {
			return err
		}
	}

	return cb(cur)
}

func (cur treeCursor) valid() bool {
	return len(cur) > 0 &&
		cur[tip].idx >= 0 &&
		cur[tip].idx < cur[tip].nd.nodeCount()
}

func (cur treeCursor) parent() treeCursor {
	return cur[1:]
}

// current returns the item at the current cursor position
func (cur treeCursor) current() nodeItem {
	d.PanicIfFalse(cur.valid())
	return cur[tip].nd.getItem(cur[tip].idx)
}

func (cur treeCursor) firstItem() nodeItem {
	return cur[tip].nd.getItem(0)
}

func (cur treeCursor) lastItem() nodeItem {
	return cur[tip].nd.getItem(cur.lastIdx())
}

func (cur treeCursor) skipToNodeStart() {
	cur[tip].idx = 0
}

func (cur treeCursor) skipToNodeEnd() {
	cur[tip].idx = cur.lastIdx()
}

func (cur treeCursor) atNodeStart() bool {
	return cur[tip].idx == 0
}

func (cur treeCursor) atNodeEnd() bool {
	return cur[tip].idx == cur.lastIdx()
}

func (cur treeCursor) lastIdx() int {
	return cur[tip].nd.nodeCount() - 1
}

func (cur treeCursor) level() uint64 {
	return uint64(cur[tip].nd.level())
}

func (cur treeCursor) nrw() NodeReadWriter {
	return cur[tip].nrw
}

func (cur treeCursor) seek(ctx context.Context, nrw NodeReadWriter, item nodeItem, cb compareItems) (err error) {
	inBounds := true
	if cur.parent().valid() {
		inBounds = cb(item, cur.firstItem()) >= 0 || cb(item, cur.lastItem()) <= 0
	}

	if !inBounds {
		// |item| is outside the bounds of |cur.nd|, search up the tree
		err = cur.parent().seek(ctx, nrw, item, cb)
		if err != nil {
			return err
		}

		cur[tip].nd, err = fetchRef(ctx, nrw, cur.parent().current())
		if err != nil {
			return err
		}
	}

	cur[tip].idx = cur.search(item, cb)

	return
}

// search returns the index of |item| if it's present in |cur.nd|, or the
// index of the next greatest element if it is not present.
func (cur treeCursor) search(item nodeItem, cb compareItems) int {
	// todo(andy): this is specific to Map
	if cur.level() == 0 {
		// leaf nodes
		idx := sort.Search(cur[tip].nd.nodeCount()/2, func(i int) bool {
			it := cur[tip].nd.getItem(i * 2)
			cmp := cb(item, it)
			return cmp <= 0
		})
		return idx * 2
	} else {
		// internal nodes
		return sort.Search(cur[tip].nd.nodeCount(), func(i int) bool {
			it := cur[tip].nd.getItem(i)
			cmp := cb(item, it)
			return cmp <= 0
		})
	}
}

func (cur treeCursor) validateNode(cb compareItems) {
	if cur.level() == 0 {
		for i := 2; i < cur[tip].nd.nodeCount(); i += 2 {
			prev := cur[tip].nd.getItem(i - 2)
			curr := cur[tip].nd.getItem(i)
			if cb(prev, curr) != -1 {
				panic("")
			}
		}
	} else {
		for i := 1; i < cur[tip].nd.nodeCount(); i++ {
			prev := cur[tip].nd.getItem(i - 1)
			curr := cur[tip].nd.getItem(i)
			if cb(prev, curr) != -1 {
				panic("")
			}
		}
	}
}

func (cur treeCursor) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur treeCursor) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
	if cur[tip].idx < cur[tip].nd.nodeCount()-1 {
		cur[tip].idx++
		return true, nil
	}

	if cur[tip].idx == cur[tip].nd.nodeCount() {
		return false, nil
	}

	if cur.parent().valid() {
		ok, err := cur.parent().advanceMaybeAllowPastEnd(ctx, false)

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
		cur[tip].idx++
	}

	return false, nil
}

func (cur treeCursor) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur treeCursor) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
	if cur[tip].idx > 0 {
		cur[tip].idx--
		return true, nil
	}

	if cur[tip].idx == -1 {
		return false, nil
	}

	d.PanicIfFalse(0 == cur[tip].idx)

	if cur.parent().valid() {
		ok, err := cur.parent().retreatMaybeAllowBeforeStart(ctx, false)

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
		cur[tip].idx--
	}

	return false, nil
}

// fetchNode loads the Node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur treeCursor) fetchNode(ctx context.Context) (err error) {
	d.PanicIfFalse(cur.parent().valid())
	cur[tip].nd, err = fetchRef(ctx, cur.nrw(), cur.parent().current())
	cur[tip].idx = -1 // caller must set
	return err
}

func (cur treeCursor) compare(other treeCursor) int {
	if cur.parent().valid() {
		p := cur.parent().compare(other.parent())
		if p != 0 {
			return p
		}
	}
	d.PanicIfFalse(cur[tip].nd.nodeCount() == other[tip].nd.nodeCount())
	return cur[tip].idx - other[tip].idx
}
