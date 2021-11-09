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
)

//  newSplitterFn makes a nodeSplitter.
type newSplitterFn func(salt byte) nodeSplitter

type treeChunker struct {
	cur     *nodeCursor
	current []nodeItem
	currSz  uint64
	done    bool

	level  uint64
	parent *treeChunker

	splitter nodeSplitter
	newSplit newSplitterFn

	ns NodeStore
}

func newEmptyTreeChunker(ctx context.Context, ns NodeStore, newSplit newSplitterFn) (*treeChunker, error) {
	levelZero := uint64(0)
	return newTreeChunker(ctx, nil, levelZero, ns, newSplit)
}

func newTreeChunker(ctx context.Context, cur *nodeCursor, level uint64, ns NodeStore, newSplit newSplitterFn) (*treeChunker, error) {
	// |cur| will be nil if this is a new Node, implying this is a new tree, or the tree has grown in height relative
	// to its original chunked form.

	sc := &treeChunker{
		cur:      cur,
		current:  make([]nodeItem, 0, 1<<10),
		level:    level,
		parent:   nil,
		newSplit: newSplit,
		splitter: newSplit(byte(level % 256)),
		ns:       ns,
	}

	if cur != nil {
		err := sc.resume(ctx)

		if err != nil {
			return nil, err
		}
	}

	return sc, nil
}

func (tc *treeChunker) resume(ctx context.Context) (err error) {
	if tc.cur.parent != nil && tc.parent == nil {
		if err := tc.createParentChunker(ctx); err != nil {
			return err
		}
	}

	idx := tc.cur.idx
	tc.cur.skipToNodeStart()

	for tc.cur.idx < idx {
		pair := tc.cur.currentPair()

		_, err = tc.Append(ctx, pair.key(), pair.value())
		if err != nil {
			return err
		}

		_, err = tc.cur.advance(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// advanceTo advances the treeChunker to the next "spine" at which modifications
// to and existing prolly-tree should take place.
func (tc *treeChunker) advanceTo(ctx context.Context, next *nodeCursor) error {
	// todo(andy): edit
	// There are four basic situations which must be handled when advancing to a
	// new chunking position:
	//
	// Case (1): |tc.cur| and |next| are exactly aligned. In this case, there's
	//           nothing to do. Just assign tc.cur = next.
	//
	// Case (2): |tc.cur| is "ahead" of |next|. This can only have resulted from
	//           advancing of a lower level causing |tc.cur| to forward. In this
	//           case, we forward |next| until the cursors are aligned and then
	//           process as if Case (1):
	//
	// Case (3+4): |tc.cur| is "behind" |next|, we must consume elements in
	//             |tc.cur| until either:
	//
	//   Case (3): |tc.cur| aligns with |next|. In this case, we just assign
	//             tc.cur = next.
	//   Case (4): A boundary is encountered which is aligned with a boundary
	//             in the previous state. This is the critical case, as is allows
	//             us to skip over large parts of the tree. In this case, we align
	//             parent chunkers then tc.resume() at |next|

	cmp := tc.cur.compare(next)

	if cmp == 0 { // Case (1)
		return nil
	}

	if cmp > 0 { // Case (2)
		for tc.cur.compare(next) > 0 {
			if _, err := next.advance(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	fastForward := false

	for tc.cur.compare(next) < 0 { // Case (4) or (3)

		// append items until we catchup with |next|, or until
		// we resynchronize with the previous tree.
		pair := tc.cur.currentPair()
		ok, err := tc.Append(ctx, pair.key(), pair.value())
		if err != nil {
			return err
		}

		// Note: if |ok| is true, but |tc.cur.atNodeEnd()| is false,
		// then we've de-synchronized with the previous tree.

		if ok && tc.cur.atNodeEnd() { // re-synchronized at |tc.level|

			if tc.cur.parent == nil {
				if tc.cur.parent.compare(next.parent) < 0 { // Case (4)
					// |tc| re-synchronized at |tc.level|, but we're still behind |next|.
					// We can advance |tc| at level+1 to get to |next| faster.
					fastForward = true
				}

				// Here we need to advance the chunker's cursor, but calling
				// tc.cur.forward() would needlessly fetch another chunk at the
				// current level. Instead, we only advance the parent.
				_, err := tc.cur.parent.advanceInBounds(ctx)
				if err != nil {
					return err
				}

				// |tc.cur| is now inconsistent with its parent, invalidate it.
				tc.cur.nd = nil
			}

			break
		}

		if _, err := tc.cur.advance(ctx); err != nil {
			return err
		}
	}

	if tc.parent != nil && next.parent != nil {
		// At this point we've either caught up to |next|, or we've
		// re-synchronized at |tc.level| and we're fast-forwarding
		// at the next level up in the tree.
		err := tc.parent.advanceTo(ctx, next.parent)
		if err != nil {
			return err
		}
	}

	// We may have invalidated cursors as we re-synchronized,
	// so copy |next| here.
	tc.cur.copy(next)

	if fastForward { // Case (4)
		// we fast-forwarded to the current chunk, so we
		// need to process its prefix
		if err := tc.resume(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (tc *treeChunker) Skip(ctx context.Context) error {
	_, err := tc.cur.advance(ctx)
	return err
}

// Append adds a new key-value pair to the chunker, validating the new pair to ensure
// that chunks are well-formed. Key-value pairs are appended atomically a chunk boundary
// may be made before or after the pair, but not between them.
func (tc *treeChunker) Append(ctx context.Context, key, value nodeItem) (bool, error) {
	// When adding new key-value pairs to an in-progress chunk, we must enforce 3 invariants
	// (1) Key-value pairs are stored in the same Node.
	// (2) The total size of a Node's data cannot exceed |maxNodeDataSize|.
	// (3) Internal Nodes (level > 0) must contain at least 2 key-value pairs (4 node items).
	//     Infinite recursion can occur if internal nodes contain a single metaPair with a key
	//     large enough to trigger a chunk boundary. Forming a chunk boundary after a single
	//     key will lead to an identical metaPair in the next level in the tree, triggering
	//     the same state infinitely. This problem can only occur at levels 2 and above,
	//     but we enforce this constraint for all internal nodes of the tree.

	// constraint (3)
	degenerate := !tc.isLeaf() && len(tc.current) < metaPairCount*2

	// constraint (2)
	overflow := false
	sum := tc.currSz + uint64(len(key)+len(value))
	if sum >= maxNodeDataSize {
		overflow = true
	}

	if overflow && degenerate {
		// Constraints (2) and (3) are in conflict
		panic("impossible node")
	}

	if overflow {
		// Enforce constraints (1) and (2):
		//  |key| and |value| won't fit in this chunk, force a
		//  boundary here and pass them to the next chunk.
		err := tc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}
	}

	tc.current = append(tc.current, key, value)
	tc.currSz += uint64(len(key) + len(value))
	err := tc.splitter.Append(key, value)
	if err != nil {
		return false, err
	}

	// recompute with updated |tc.current|
	degenerate = !tc.isLeaf() && len(tc.current) < metaPairCount*2

	if tc.splitter.CrossedBoundary() && !degenerate {
		err := tc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (tc *treeChunker) handleChunkBoundary(ctx context.Context) error {
	assertTrue(len(tc.current) > 0)
	tc.splitter.Reset()

	if tc.parent == nil {
		err := tc.createParentChunker(ctx)
		if err != nil {
			return err
		}
	}

	_, meta, err := tc.createNode(ctx)
	if err != nil {
		return err
	}

	_, err = tc.parent.Append(ctx, meta.key(), meta.value())
	if err != nil {
		return err
	}

	return nil
}

func (tc *treeChunker) createParentChunker(ctx context.Context) (err error) {
	assertTrue(tc.parent == nil)

	var parent *nodeCursor
	if tc.cur != nil && tc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.forward() will affect our parent - and vice versa -
		// in surprising ways. Instead, Skip moves forward our parent's cursor if we forward across a boundary.
		parent = tc.cur.parent
	}

	tc.parent, err = newTreeChunker(ctx, parent, tc.level+1, tc.ns, tc.newSplit)
	if err != nil {
		return err
	}

	return nil
}

// createNode creates a Node from the current items in |sc.currentPair|,
// clears the current items, then returns the new Node and a metaValue that
// points to it. The Node is always eagerly written.
func (tc *treeChunker) createNode(ctx context.Context) (Node, nodePair, error) {
	nd, metaPair, err := writeNewChild(ctx, tc.ns, tc.level, tc.current...)
	if err != nil {
		return nil, nodePair{}, err
	}

	// |tc.currentPair| is copied so it's safe to re-use the memory.
	tc.current = tc.current[:0]
	tc.currSz = 0

	return nd, metaPair, nil
}

// Done returns the root Node of the resulting tree.
// The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (tc *treeChunker) Done(ctx context.Context) (Node, error) {
	assertTrue(!tc.done)
	tc.done = true

	if tc.cur != nil {
		err := tc.finalizeCursor(ctx)

		if err != nil {
			return nil, err
		}
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent
	// to find the root of the resulting tree.
	if tc.parent != nil && tc.parent.anyPending() {
		if len(tc.current) > 0 {
			// If there are items in |currentPair| at this point, they represent the final items of the Node which occurred
			// beyond the previous *explicit* chunk boundary. The end of input of a Node is considered an *implicit*
			// boundary.
			err := tc.handleChunkBoundary(ctx)
			if err != nil {
				return nil, err
			}
		}

		return tc.parent.Done(ctx)
	}

	// At this point, we know this nodeSplitter contains, in |currentPair| every item at this level of the resulting tree.
	// To see this, consider that there are two ways a nodeSplitter can enter items into its |currentPair|:
	//  (1) as the result of resume() with the cursor on anything other than the first item in the Node, and
	//  (2) as a result of a child nodeSplitter hitting an explicit chunk boundary during either Append() or finalize().
	// The only way there can be no items in some parent nodeSplitter's |currentPair| is if this nodeSplitter began with
	// cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and
	// continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any
	// items up to a parent as a result of chunking). Therefore, this nodeSplitter's currentPair must contain all items
	// within the currentPair Node.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three possible cases:
	// (1) This is "leaf" nodeSplitter and thus produced tree of depth 1 which contains exactly one chunk
	//     (never hit a boundary), or
	// (2) This in an internal Node of the tree which contains multiple references to child nodes. In either case,
	//     this is the canonical root of the tree.
	if tc.isLeaf() || len(tc.current) > metaPairCount {
		nd, _, err := tc.createNode(ctx)

		if err != nil {
			return nil, err
		}

		return nd, nil
	}

	// (3) This is an internal Node of the tree which contains a single reference to a child Node. This can occur if a
	//     non-leaf nodeSplitter happens to chunk on the first item (metaValue) appended. In this case, this is the root
	//     of the tree, but it is *not* canonical, and we must walk down until we find cases (1) or (2), above.
	assertTrue(!tc.isLeaf() && len(tc.current) == metaPairCount)

	mt := metaValue(tc.current[metaPairValIdx])
	for {
		child, err := fetchChild(ctx, tc.ns, mt)
		if err != nil {
			return nil, err
		}

		if child.leafNode() || child.nodeCount() > 1 {
			return child, nil
		}

		mt = metaValue(child.getItem(metaPairValIdx))
	}
}

// If we are mutating an existing Node, appending subsequent items in the Node until we reach a pre-existing chunk
// boundary or the end of the Node.
func (tc *treeChunker) finalizeCursor(ctx context.Context) (err error) {
	for tc.cur.valid() {
		pair := tc.cur.currentPair()

		var ok bool
		ok, err = tc.Append(ctx, pair.key(), pair.value())
		if err != nil {
			return err
		}
		if ok && tc.cur.atNodeEnd() {
			break // boundary occurred at same place in old & new Node
		}

		_, err = tc.cur.advance(ctx)
		if err != nil {
			return err
		}
	}

	if tc.cur.parent != nil {
		_, err := tc.cur.parent.advance(ctx)

		if err != nil {
			return err
		}

		// Invalidate this cursor, since it is now inconsistent with its parent
		tc.cur.parent = nil
		tc.cur.nd = nil
	}

	return nil
}

// Returns true if this nodeSplitter or any of its parents have any pending items in their |currentPair| slice.
func (tc *treeChunker) anyPending() bool {
	if len(tc.current) > 0 {
		return true
	}

	if tc.parent != nil {
		return tc.parent.anyPending()
	}

	return false
}

func (tc *treeChunker) isLeaf() bool {
	return tc.level == 0
}
