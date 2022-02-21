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

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	nodeItemAccumulatorSz = 256
)

//  newSplitterFn makes a nodeSplitter.
type newSplitterFn func(salt byte) nodeSplitter

type treeChunker struct {
	cur        *nodeCursor
	keys, vals []nodeItem
	currSz     uint64
	done       bool

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
		keys:     make([]nodeItem, 0, nodeItemAccumulatorSz),
		vals:     make([]nodeItem, 0, nodeItemAccumulatorSz),
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
		_, err = tc.Append(ctx, tc.cur.currentKey(), tc.cur.currentValue())
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

// advanceTo advances the treeChunker to |nextMutation|, the nextMutation mutation point.
func (tc *treeChunker) advanceTo(ctx context.Context, next *nodeCursor) error {
	// There a four cases to handle when advancing the tree chunker
	//  (1) |tc.cur| and |nextMutation| are aligned, we're done
	//
	//  (2) |tc.cur| is "ahead" of |nextMutation|. This can be caused by advances
	//      at a lower level of the tree. In this case, advance |nextMutation|
	//      until it is even with |tc.cur|.
	//
	//  (3) |tc.cur| is behind |nextMutation|, we must consume elements between the
	//      two cursors until |tc.cur| catches up with |nextMutation|.
	//
	//  (4) This is a special case of (3) where we can "Fast-Forward" |tc.cur|
	//      towards |nextMutation|. As we consume elements between the two cursors, if
	//      we re-synchronize with the previous tree, we can skip over the
	//      chunks between the re-synchronization boundary and |nextMutation|.

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

	for tc.cur.compare(next) < 0 { // Case (3) or (4)

		// append items until we catchup with |nextMutation|, or until
		// we resynchronize with the previous tree.
		ok, err := tc.Append(ctx, tc.cur.currentKey(), tc.cur.currentValue())
		if err != nil {
			return err
		}

		// Note: if |ok| is true, but |tc.cur.atNodeEnd()| is false,
		// then we've de-synchronized with the previous tree.

		if ok && tc.cur.atNodeEnd() { // re-synchronized at |tc.level|

			if tc.cur.parent != nil {
				if tc.cur.parent.compare(next.parent) < 0 { // Case (4)
					// |tc| re-synchronized at |tc.level|, but we're still behind |nextMutation|.
					// We can advance |tc| at level+1 to get to |nextMutation| faster.
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
				tc.cur.nd = Node{}
			}

			break
		}

		if _, err := tc.cur.advance(ctx); err != nil {
			return err
		}
	}

	if tc.parent != nil && next.parent != nil {
		// At this point we've either caught up to |nextMutation|, or we've
		// re-synchronized at |tc.level| and we're fast-forwarding
		// at the nextMutation level up in the tree.
		err := tc.parent.advanceTo(ctx, next.parent)
		if err != nil {
			return err
		}
	}

	// We may have invalidated cursors as we re-synchronized,
	// so copy |nextMutation| here.
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
	// (2) The total size of a Node's data cannot exceed |maxVectorOffset|.
	// (3) Internal Nodes (level > 0) must contain at least 2 key-value pairs (4 node items).
	//     Infinite recursion can occur if internal nodes contain a single metaPair with a key
	//     large enough to trigger a chunk boundary. Forming a chunk boundary after a single
	//     key will lead to an identical metaPair in the nextMutation level in the tree, triggering
	//     the same state infinitely. This problem can only occur at levels 2 and above,
	//     but we enforce this constraint for all internal nodes of the tree.

	// constraint (3)
	degenerate := !tc.isLeaf() && len(tc.keys) == 1

	// constraint (2)
	overflow := false
	sum := tc.currSz + uint64(len(key)+len(value))
	if sum >= maxVectorOffset {
		overflow = true
	}

	if overflow && degenerate {
		// Constraints (2) and (3) are in conflict
		panic("impossible node")
	}

	if overflow {
		// Enforce constraints (1) and (2):
		//  |key| and |value| won't fit in this chunk, force a
		//  boundary here and pass them to the nextMutation chunk.
		err := tc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}
	}

	tc.keys = append(tc.keys, key)
	tc.vals = append(tc.vals, value)
	tc.currSz += uint64(len(key) + len(value))
	err := tc.splitter.Append(key, value)
	if err != nil {
		return false, err
	}

	// recompute with updated |tc.keys|
	degenerate = !tc.isLeaf() && len(tc.keys) == 1

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
	assertTrue(len(tc.keys) > 0)
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

	k, r := meta.key(), meta.ref()
	_, err = tc.parent.Append(ctx, nodeItem(k), nodeItem(r[:]))
	if err != nil {
		return err
	}

	return nil
}

func (tc *treeChunker) createParentChunker(ctx context.Context) (err error) {
	assertTrue(tc.parent == nil)

	var parent *nodeCursor
	if tc.cur != nil && tc.cur.parent != nil {
		// todo(andy): does this comment make sense? cloning a pointer?
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

// createNode creates a Node from the keys items in |sc.currentPair|,
// clears the keys items, then returns the new Node and a metaValue that
// points to it. The Node is always eagerly written.
func (tc *treeChunker) createNode(ctx context.Context) (Node, metaPair, error) {
	nd, mp, err := writeNewChild(ctx, tc.ns, tc.level, tc.keys, tc.vals)
	if err != nil {
		return Node{}, metaPair{}, err
	}

	// buffers are copied, it's safe to re-use the memory.
	tc.keys = tc.keys[:0]
	tc.vals = tc.vals[:0]
	tc.currSz = 0

	return nd, mp, nil
}

// Done returns the root Node of the resulting tree.
// The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (tc *treeChunker) Done(ctx context.Context) (Node, error) {
	assertTrue(!tc.done)
	tc.done = true

	if tc.cur != nil {
		if err := tc.finalizeCursor(ctx); err != nil {
			return Node{}, err
		}
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent
	// to find the root of the resulting tree.
	if tc.parent != nil && tc.parent.anyPending() {
		if len(tc.keys) > 0 {
			// |tc.keys| are the last items at this level of the tree,
			// make a chunk out of them
			if err := tc.handleChunkBoundary(ctx); err != nil {
				return Node{}, err
			}
		}

		return tc.parent.Done(ctx)
	}

	// At this point, we know |tc.keys| contains every item at this level of the tree.
	// To see this, consider that there are two ways items can enter |tc.keys|.
	//  (1) as the result of resume() with the cursor on anything other than the first item in the Node
	//  (2) as a result of a child treeChunker hitting an explicit chunk boundary during either Append() or finalize().
	//
	// The only way there can be no items in some parent treeChunker's |tc.keys| is if this treeChunker began with
	// a cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and
	// continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any
	// items up to a parent as a result of chunking). Therefore, this treeChunker's |tc.keys| must contain all items
	// within the current Node.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three possible cases:
	// (1) This is "leaf" treeChunker and thus produced tree of depth 1 which contains exactly one chunk
	//     (never hit a boundary), or
	// (2) This in an internal Node of the tree which contains multiple references to child nodes. In either case,
	//     this is the canonical root of the tree.
	if tc.isLeaf() || len(tc.keys) > 1 {
		nd, _, err := tc.createNode(ctx)
		if err != nil {
			return Node{}, err
		}
		return nd, nil
	}

	// (3) This is an internal Node of the tree with a single metaPair. This is a non-canonical root, and we must walk
	//     down until we find cases (1) or (2), above.
	assertTrue(!tc.isLeaf())
	assertTrue(len(tc.keys) == 1)

	mt := hash.New(tc.vals[0])
	for {
		child, err := fetchChild(ctx, tc.ns, mt)
		if err != nil {
			return Node{}, err
		}

		if child.leafNode() || child.count > 1 {
			return child, nil
		}

		mt = child.getRef(0)
	}
}

// If we are mutating an existing Node, appending subsequent items in the Node until we reach a pre-existing chunk
// boundary or the end of the Node.
func (tc *treeChunker) finalizeCursor(ctx context.Context) (err error) {
	for tc.cur.valid() {
		var ok bool
		ok, err = tc.Append(ctx, tc.cur.currentKey(), tc.cur.currentValue())
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

		// invalidate this cursor to mark it finalized.
		tc.cur.nd = Node{}
	}

	return nil
}

// Returns true if this nodeSplitter or any of its parents have any pending items in their |currentPair| slice.
func (tc *treeChunker) anyPending() bool {
	if len(tc.keys) > 0 {
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
