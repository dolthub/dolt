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
		pair := sc.cur.currentPair()

		_, err = sc.Append(ctx, pair.key(), pair.value())
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

// advanceTo advances the treeChunker to the next "spine" at which modifications
// to and existing prolly-tree should take place.
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
		pair := sc.cur.currentPair()

		ok, err := sc.Append(ctx, pair.key(), pair.value())
		if err != nil {
			return err
		}
		if ok && sc.cur.atNodeEnd() {
			if sc.cur.parent != nil {

				if sc.cur.parent.compare(next.parent) < 0 {
					// Case (4): We stopped consuming items on this level before entering
					// the Node referenced by |next|
					reachedNext = false
				}

				// Note: Logically, what is happening here is that we are consuming the
				// item at the current level. Logically, we'd call sc.cur.forward(),
				// but that would force loading of the next Node, which we don't
				// need for any reason, so instead we forward the parent and take care
				// not to allow it to step outside the Node.
				_, err := sc.cur.parent.advanceInBounds(ctx)
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

func (sc *treeChunker) Skip(ctx context.Context) error {
	_, err := sc.cur.advance(ctx)
	return err
}

// Append adds a new key-value pair to the chunker, validating the new pair to ensure
// that chunks are well-formed. Key-value pairs are appended atomically a chunk boundary
// may be made before or after the pair, but not between them.
func (sc *treeChunker) Append(ctx context.Context, key, value nodeItem) (bool, error) {
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
	degenerate := !sc.isLeaf() && len(sc.current) < metaPairCount*2

	// constraint (2)
	overflow := false
	sum := sc.currSz + uint64(len(key)+len(value))
	if sum >= maxNodeDataSize {
		overflow = true
	}

	if overflow && degenerate {
		// Constraints (2) and (3) are in conflict
		panic("impossible node")
	}

	if overflow {
		// Enforce constraints (1) and (2):
		//  |key| and |value| won't fit in this chunk,
		//  force a boundary here and pass them as leftovers
		err := sc.handleChunkBoundary(ctx, key, value)
		if err != nil {
			return false, err
		}
	}

	sc.current = append(sc.current, key, value)
	sc.currSz += uint64(len(key) + len(value))
	err := sc.splitter.Append(key, value)
	if err != nil {
		return false, err
	}

	// recompute with updated |sc.current|
	degenerate = !sc.isLeaf() && len(sc.current) < metaPairCount*2

	if sc.splitter.CrossedBoundary() && !degenerate {
		err := sc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (sc *treeChunker) handleChunkBoundary(ctx context.Context, leftovers ...nodeItem) error {
	assertTrue(len(sc.current) > 0)
	sc.splitter.Reset()

	if sc.parent == nil {
		err := sc.createParent(ctx)
		if err != nil {
			return err
		}
	}

	_, meta, err := sc.createNode(ctx)
	if err != nil {
		return err
	}

	_, err = sc.parent.Append(ctx, meta.key(), meta.value())
	if err != nil {
		return err
	}

	// elements in |leftover|, if any exist, were too big to fit
	// in the previous chunk. We'll add them to the next chunk.
	sc.current = append(sc.current, leftovers...)

	sc.currSz = 0
	for i := range leftovers {
		sc.currSz += uint64(len(leftovers[i]))
	}

	return nil
}

func (sc *treeChunker) createParent(ctx context.Context) (err error) {
	assertTrue(sc.parent == nil)
	var parent *nodeCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.forward() will affect our parent - and vice versa -
		// in surprising ways. Instead, Skip moves forward our parent's cursor if we forward across a boundary.
		parent = sc.cur.parent
	}

	sc.parent, err = newTreeChunker(ctx, parent, sc.level+1, sc.ns, sc.newSplit)
	if err != nil {
		return err
	}

	return nil
}

// createNode creates a Node from the current items in |sc.currentPair|,
// clears the current items, then returns the new Node and a metaValue that
// points to it. The Node is always eagerly written.
func (sc *treeChunker) createNode(ctx context.Context) (Node, nodePair, error) {
	nd, metaPair, err := writeNewChild(ctx, sc.ns, sc.level, sc.current...)
	if err != nil {
		return nil, nodePair{}, err
	}

	// |sc.currentPair| is copied so it's safe to re-use the memory.
	sc.current = sc.current[:0]

	return nd, metaPair, nil
}

// Done returns the root Node of the resulting tree.
// The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (sc *treeChunker) Done(ctx context.Context) (Node, error) {
	assertTrue(!sc.done)
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
			// If there are items in |currentPair| at this point, they represent the final items of the Node which occurred
			// beyond the previous *explicit* chunk boundary. The end of input of a Node is considered an *implicit*
			// boundary.
			err := sc.handleChunkBoundary(ctx)
			if err != nil {
				return nil, err
			}
		}

		return sc.parent.Done(ctx)
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
	if sc.isLeaf() || len(sc.current) > metaPairCount {
		nd, _, err := sc.createNode(ctx)

		if err != nil {
			return nil, err
		}

		return nd, nil
	}

	// (3) This is an internal Node of the tree which contains a single reference to a child Node. This can occur if a
	//     non-leaf nodeSplitter happens to chunk on the first item (metaValue) appended. In this case, this is the root
	//     of the tree, but it is *not* canonical, and we must walk down until we find cases (1) or (2), above.
	assertTrue(!sc.isLeaf() && len(sc.current) == metaPairCount)

	mt := metaValue(sc.current[metaPairValIdx])
	for {
		child, err := fetchChild(ctx, sc.ns, mt)
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
func (sc *treeChunker) finalizeCursor(ctx context.Context) (err error) {
	for sc.cur.valid() {
		pair := sc.cur.currentPair()

		if ok, err := sc.Append(ctx, pair.key(), pair.value()); err != nil {
			return err
		} else if ok && sc.cur.atNodeEnd() {
			break // boundary occurred at same place in old & new Node
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

// Returns true if this nodeSplitter or any of its parents have any pending items in their |currentPair| slice.
func (sc *treeChunker) anyPending() bool {
	if len(sc.current) > 0 {
		return true
	}

	if sc.parent != nil {
		return sc.parent.anyPending()
	}

	return false
}

func (sc *treeChunker) isLeaf() bool {
	return sc.level == 0
}
