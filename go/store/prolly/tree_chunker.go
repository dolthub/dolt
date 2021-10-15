// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/d"
)

//  newSplitterFn makes a nodeSplitter.
type newSplitterFn func(salt byte) nodeSplitter

type treeChunker struct {
	cur     *nodeCursor
	current []nodeItem
	done    bool

	level  uint64
	parent *treeChunker

	splitter nodeSplitter
	newSplit newSplitterFn

	nrw NodeReadWriter
}

func newEmptyTreeChunker(ctx context.Context, vrw NodeReadWriter, newCh newSplitterFn) (*treeChunker, error) {
	return newTreeChunker(ctx, nil, uint64(0), vrw, newCh)
}

func newTreeChunker(ctx context.Context, cur *nodeCursor, level uint64, vrw NodeReadWriter, newSplit newSplitterFn) (*treeChunker, error) {
	// |cur| will be nil if this is a new node, implying this is a new tree, or the tree has grown in height relative
	// to its original chunked form.

	sc := &treeChunker{
		cur:      cur,
		current:  make([]nodeItem, 0, 1<<10),
		level:    level,
		parent:   nil,
		newSplit: newSplit,
		splitter: newSplit(byte(level % 256)),
		nrw:      vrw,
	}

	if cur != nil {
		err := sc.resume(ctx)

		if err != nil {
			return nil, err
		}
	}

	return sc, nil
}

func (sc *treeChunker) isLeaf() bool {
	return sc.level == 0
}

func (sc *treeChunker) resume(ctx context.Context) (err error) {
	if sc.cur.parent != nil && sc.parent == nil {
		err := sc.createParent(ctx)

		if err != nil {
			return err
		}
	}

	idx := sc.cur.idx
	sc.cur.skipToNodeStart()

	for sc.cur.idx < idx {
		item := sc.cur.current()

		_, err = sc.Append(ctx, item)
		if err != nil {
			return err
		}

		_, err = sc.cur.advance(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// advanceTo advances the treeChunker to the next "spine" at which
// modifications to the prolly-tree should take place
func (sc *treeChunker) advanceTo(ctx context.Context, next *nodeCursor) error {
	// There are four basic situations which must be handled when advancing to a
	// new chunking position:
	//
	// Case (1): |sc.cur| and |next| are exactly aligned. In this case, there's
	//           nothing to do. Just assign sc.cur = next.
	//
	// Case (2): |sc.cur| is "ahead" of |next|. This can only have resulted from
	//           advancing of a lower level causing |sc.cur| to forward. In this
	//           case, we forward |next| until the cursors are aligned and then
	//           process as if Case (1):
	//
	// Case (3+4): |sc.cur| is "behind" |next|, we must consume elements in
	//             |sc.cur| until either:
	//
	//   Case (3): |sc.cur| aligns with |next|. In this case, we just assign
	//             sc.cur = next.
	//   Case (4): A boundary is encountered which is aligned with a boundary
	//             in the previous state. This is the critical case, as is allows
	//             us to skip over large parts of the tree. In this case, we align
	//             parent chunkers then sc.resume() at |next|

	for sc.cur.compare(next) > 0 {
		_, err := next.advance(ctx) // Case (2)
		if err != nil {
			return err
		}
	}

	// If neither loop above and below are entered, it is Case (1). If the loop
	// below is entered but Case (4) isn't reached, then it is Case (3).
	reachedNext := true
	for sc.cur.compare(next) < 0 {
		item := sc.cur.current()

		if ok, err := sc.Append(ctx, item); err != nil {
			return err
		} else if ok && sc.cur.atNodeEnd() {
			if sc.cur.parent != nil {

				if sc.cur.parent.compare(next.parent) < 0 {
					// Case (4): We stopped consuming items on this level before entering
					// the node referenced by |next|
					reachedNext = false
				}

				// Note: Logically, what is happening here is that we are consuming the
				// item at the current level. Logically, we'd call sc.cur.forward(),
				// but that would force loading of the next node, which we don't
				// need for any reason, so instead we forward the parent and take care
				// not to allow it to step outside the node.
				_, err := sc.cur.parent.advanceMaybeAllowPastEnd(ctx, false)
				if err != nil {
					return err
				}

				// Invalidate this cursor, since it is now inconsistent with its parent
				sc.cur.parent = nil
				sc.cur.nd = nil
			}

			break
		}

		if _, err := sc.cur.advance(ctx); err != nil {
			return err
		}
	}

	if sc.parent != nil && next.parent != nil {
		err := sc.parent.advanceTo(ctx, next.parent)

		if err != nil {
			return err
		}
	}

	sc.cur = next
	if !reachedNext {
		err := sc.resume(ctx) // Case (4)

		if err != nil {
			return err
		}
	}

	return nil
}

func (sc *treeChunker) Append(ctx context.Context, items ...nodeItem) (bool, error) {
	sc.current = append(sc.current, items...)

	for _, item := range items {
		if err := sc.splitter.Append(item); err != nil {
			return false, err
		}
	}

	if sc.splitter.CrossedBoundary() {
		// When a metaTuple contains a key that is so large that it causes a chunk boundary to be crossed simply by
		// encoding the metaTuple then we will create a metaTuple to encode the metaTuple containing the same key again,
		// and again crossing a chunk boundary causes infinite recursion.
		// The solution is not to allow a metaTuple with a single leaf to cross chunk boundaries.
		if sc.level > 0 && len(sc.current) == 1 {
			return false, nil
		}

		err := sc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}

		return true, nil
	}
	return false, nil
}

func (sc *treeChunker) Skip(ctx context.Context) error {
	_, err := sc.cur.advance(ctx)
	return err
}

func (sc *treeChunker) createParent(ctx context.Context) (err error) {
	d.PanicIfFalse(sc.parent == nil)
	var parent *nodeCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.forward() will affect our parent - and vice versa -
		// in surprising ways. Instead, Skip moves forward our parent's cursor if we forward across a boundary.
		parent = sc.cur.parent
	}

	sc.parent, err = newTreeChunker(ctx, parent, sc.level+1, sc.nrw, sc.newSplit)
	if err != nil {
		return err
	}

	return nil
}

// createNode creates a node from the current items in |sc.current|,
// clears the current items, then returns the new node and a metaTuple that
// points to it. The node is always eagerly written.
func (sc *treeChunker) createNode(ctx context.Context) (node, nodeItem, error) {
	nd, meta, err := writeNewNode(ctx, sc.nrw, sc.level, sc.current...)
	if err != nil {
		return nil, nil, err
	}

	// |sc.current| is copied so it's safe to re-use the memory.
	sc.current = sc.current[:0]

	return nd, nodeItem(meta), nil
}

func (sc *treeChunker) handleChunkBoundary(ctx context.Context) error {
	d.PanicIfFalse(len(sc.current) > 0)
	sc.splitter.Reset()

	if sc.parent == nil {
		err := sc.createParent(ctx)
		if err != nil {
			return err
		}
	}

	_, mt, err := sc.createNode(ctx)
	if err != nil {
		return err
	}

	_, err = sc.parent.Append(ctx, mt)
	if err != nil {
		return err
	}

	return nil
}

// Returns true if this nodeSplitter or any of its parents have any pending items in their |current| slice.
func (sc *treeChunker) anyPending() bool {
	if len(sc.current) > 0 {
		return true
	}

	if sc.parent != nil {
		return sc.parent.anyPending()
	}

	return false
}

// Done returns the root node of the resulting tree.
// The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (sc *treeChunker) Done(ctx context.Context) (node, error) {
	d.PanicIfTrue(sc.done)
	sc.done = true

	if sc.cur != nil {
		err := sc.finalizeCursor(ctx)

		if err != nil {
			return nil, err
		}
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent
	// to find the root of the resulting tree.
	if sc.parent != nil && sc.parent.anyPending() {
		if len(sc.current) > 0 {
			// If there are items in |current| at this point, they represent the final items of the node which occurred
			// beyond the previous *explicit* chunk boundary. The end of input of a node is considered an *implicit*
			// boundary.
			err := sc.handleChunkBoundary(ctx)
			if err != nil {
				return nil, err
			}
		}

		return sc.parent.Done(ctx)
	}

	// At this point, we know this nodeSplitter contains, in |current| every item at this level of the resulting tree.
	// To see this, consider that there are two ways a nodeSplitter can enter items into its |current|:
	//  (1) as the result of resume() with the cursor on anything other than the first item in the node, and
	//  (2) as a result of a child nodeSplitter hitting an explicit chunk boundary during either Append() or finalize().
	// The only way there can be no items in some parent nodeSplitter's |current| is if this nodeSplitter began with
	// cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and
	// continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any
	// items up to a parent as a result of chunking). Therefore, this nodeSplitter's current must contain all items
	// within the current node.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three possible cases:
	// (1) This is "leaf" nodeSplitter and thus produced tree of depth 1 which contains exactly one chunk
	//     (never hit a boundary), or
	// (2) This in an internal node of the tree which contains multiple references to child nodes. In either case,
	//     this is the canonical root of the tree.
	if sc.isLeaf() || len(sc.current) > 1 {
		seq, _, err := sc.createNode(ctx)

		if err != nil {
			return nil, err
		}

		return seq, nil
	}

	// (3) This is an internal node of the tree which contains a single reference to a child node. This can occur if a
	//     non-leaf nodeSplitter happens to chunk on the first item (metaTuple) appended. In this case, this is the root
	//     of the tree, but it is *not* canonical, and we must walk down until we find cases (1) or (2), above.
	d.PanicIfFalse(!sc.isLeaf() && len(sc.current) == 1)

	mt := sc.current[0]
	for {
		child, err := fetchRef(ctx, sc.nrw, mt)
		if err != nil {
			return nil, err
		}

		if child.leafNode() || child.count() > 1 {
			return child, nil
		}

		mt = child.getItem(0)
	}
}

// If we are mutating an existing node, appending subsequent items in the node until we reach a pre-existing chunk
// boundary or the end of the node.
func (sc *treeChunker) finalizeCursor(ctx context.Context) (err error) {
	for sc.cur.valid() {
		item := sc.cur.current()

		if ok, err := sc.Append(ctx, item); err != nil {
			return err
		} else if ok && sc.cur.atNodeEnd() {
			break // boundary occurred at same place in old & new node
		}

		_, err = sc.cur.advance(ctx)
		if err != nil {
			return err
		}
	}

	if sc.cur.parent != nil {
		_, err := sc.cur.parent.advance(ctx)

		if err != nil {
			return err
		}

		// Invalidate this cursor, since it is now inconsistent with its parent
		sc.cur.parent = nil
		sc.cur.nd = nil
	}

	return nil
}
