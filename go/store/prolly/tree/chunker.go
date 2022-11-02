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

	"github.com/dolthub/dolt/go/store/prolly/message"
)

type Chunker interface {
	AddPair(ctx context.Context, key, value Item) error
	Done(ctx context.Context) (Node, error)
}

type chunker[S message.Serializer] struct {
	cur    *Cursor
	parent *chunker[S]
	level  int
	done   bool

	splitter   nodeSplitter
	builder    *nodeBuilder[S]
	serializer S

	ns NodeStore
}

//var _ Chunker = &chunker[]{}

func NewEmptyChunker[S message.Serializer](ctx context.Context, ns NodeStore, serializer S) (Chunker, error) {
	return newEmptyChunker(ctx, ns, serializer)
}

func newEmptyChunker[S message.Serializer](ctx context.Context, ns NodeStore, serializer S) (*chunker[S], error) {
	return newChunker(ctx, nil, 0, ns, serializer)
}

func newChunker[S message.Serializer](ctx context.Context, cur *Cursor, level int, ns NodeStore, serializer S) (*chunker[S], error) {
	// |cur| will be nil if this is a new Node, implying this is a new tree, or the tree has grown in height relative
	// to its original chunked form.

	splitter := defaultSplitterFactory(uint8(level % 256))
	builder := newNodeBuilder(serializer, level)

	sc := &chunker[S]{
		cur:        cur,
		parent:     nil,
		level:      level,
		splitter:   splitter,
		builder:    builder,
		serializer: serializer,
		ns:         ns,
	}

	if cur != nil {
		if err := sc.processPrefix(ctx); err != nil {
			return nil, err
		}
	}

	return sc, nil
}

func (tc *chunker[S]) processPrefix(ctx context.Context) (err error) {
	if tc.cur.parent != nil && tc.parent == nil {
		if err := tc.createParentChunker(ctx); err != nil {
			return err
		}
	}

	idx := tc.cur.idx
	tc.cur.skipToNodeStart()

	for tc.cur.idx < idx {
		var sz uint64
		sz, err = tc.cur.currentSubtreeSize()
		if err != nil {
			return err
		}
		_, err = tc.append(ctx,
			tc.cur.CurrentKey(),
			tc.cur.CurrentValue(),
			sz)

		// todo(andy): seek to correct chunk
		//  currently when inserting tuples between chunks
		//  we seek to the end of the previous chunk rather
		//  than the beginning of the next chunk. This causes
		//  us to iterate over the entire previous chunk
		//assertFalse(ok)

		if err != nil {
			return err
		}

		err = tc.cur.Advance(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddPair adds a val.Tuple pair to the chunker.
func (tc *chunker[S]) AddPair(ctx context.Context, key, value Item) error {
	_, err := tc.append(ctx, Item(key), Item(value), 1)
	return err
}

// UpdatePair updates a val.Tuple pair in the chunker.
func (tc *chunker[S]) UpdatePair(ctx context.Context, key, value Item) error {
	if err := tc.skip(ctx); err != nil {
		return err
	}
	_, err := tc.append(ctx, Item(key), Item(value), 1)
	return err
}

// DeletePair deletes a val.Tuple pair from the chunker.
func (tc *chunker[S]) DeletePair(ctx context.Context, _, _ Item) error {
	return tc.skip(ctx)
}

// AdvanceTo progresses the chunker until its tracking cursor catches up with
// |next|, a cursor indicating next key where an edit will be applied.
//
// The method proceeds from the deepest chunker recursively into its
// linked list parents:
//
//	(1) If the current cursor and all of its parents are aligned with |next|,
//	we are done.
//
//	(2) In lockstep, a) append to the chunker and b) increment the cursor until
//	we either meet condition (1) and return, or we synchronize and progress to
//	(3) or (4). Synchronizing means that the current tree being built has
//	reached a chunk boundary that aligns with a chunk boundary in the old tree
//	being mutated. Synchronization means chunks between this boundary and
//	|next| at the current cursor level will be unchanged and can be skipped.
//
//	(3) All parent cursors are (1) current or (2) synchronized, or there are no
//	parents, and we are done.
//
//	(4) The parent cursors are not aligned. Recurse into the parent. After
//	parents are aligned, we need to reprocess the prefix of the current node in
//	anticipation of impending edits that may edit the current chunk. Note that
//	processPrefix is only necessary for the "fast forward" case where we
//	synchronized the tree level before reaching |next|.
func (tc *chunker[S]) AdvanceTo(ctx context.Context, next *Cursor) error {
	cmp := tc.cur.Compare(next)
	if cmp == 0 { // step (1)
		return nil
	} else if cmp > 0 {
		//todo(max): this appears to be a result of a seek() bug, where
		// we navigate to the end of the previous chunk rather than the
		// beginning of the next chunk. I think this is basically a one-off
		// error.
		for tc.cur.Compare(next) > 0 {
			if err := next.Advance(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	sz, err := tc.cur.currentSubtreeSize()
	if err != nil {
		return err
	}
	split, err := tc.append(ctx, tc.cur.CurrentKey(), tc.cur.CurrentValue(), sz)
	if err != nil {
		return err
	}

	for !(split && tc.cur.atNodeEnd()) { // step (2)
		err = tc.cur.Advance(ctx)
		if err != nil {
			return err
		}
		if cmp = tc.cur.Compare(next); cmp >= 0 {
			// we caught up before synchronizing
			return nil
		}
		sz, err := tc.cur.currentSubtreeSize()
		if err != nil {
			return err
		}
		split, err = tc.append(ctx, tc.cur.CurrentKey(), tc.cur.CurrentValue(), sz)
		if err != nil {
			return err
		}
	}

	if tc.cur.parent == nil || next.parent == nil { // step (3)
		// end of tree
		tc.cur.copy(next)
		return nil
	}

	if tc.cur.parent.Compare(next.parent) == 0 { // step (3)
		// (rare) new tree synchronized with old tree at the
		// same time as the cursor caught up to the next mutation point
		tc.cur.copy(next)
		return nil
	}

	// step(4)

	// This optimization is logically equivalent to advancing
	// current cursor. Because we just wrote a chunk, we are
	// at a boundary and can simply increment the parent.
	err = tc.cur.parent.Advance(ctx)
	if err != nil {
		return err
	}
	tc.cur.invalidateAtEnd()

	// no more pending chunks at this level, recurse
	// into parent
	err = tc.parent.AdvanceTo(ctx, next.parent)
	if err != nil {
		return err
	}

	// fast forward to the edit index at this level
	tc.cur.copy(next)

	// incoming edit can affect the entire chunk, process the prefix
	err = tc.processPrefix(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (tc *chunker[S]) skip(ctx context.Context) error {
	err := tc.cur.Advance(ctx)
	return err
}

// Append adds a new key-value pair to the chunker, validating the new pair to ensure
// that chunks are well-formed. Key-value pairs are appended atomically a chunk boundary
// may be made before or after the pair, but not between them. Returns true if chunk boundary
// was split.
func (tc *chunker[S]) append(ctx context.Context, key, value Item, subtree uint64) (bool, error) {
	// When adding new key-value pairs to an in-progress chunk, we must enforce 3 invariants
	// (1) Key-value pairs are stored in the same Node.
	// (2) The total Size of a Node's data cannot exceed |MaxVectorOffset|.
	// (3) Internal Nodes (Level > 0) must contain at least 2 key-value pairs (4 node items).
	//     Infinite recursion can occur if internal nodes contain a single novelNode with a key
	//     large enough to trigger a chunk boundary. Forming a chunk boundary after a single
	//     key will lead to an identical novelNode in the nextMutation Level in the tree, triggering
	//     the same state infinitely. This problem can only occur at levels 2 and above,
	//     but we enforce this constraint for all internal nodes of the tree.

	// constraint (3)
	degenerate := !tc.isLeaf() && tc.builder.count() == 1

	// constraint (2)
	overflow := !tc.builder.hasCapacity(key, value)

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

	tc.builder.addItems(key, value, subtree)

	err := tc.splitter.Append(key, value)
	if err != nil {
		return false, err
	}

	// recompute with updated |tc.keys|
	degenerate = !tc.isLeaf() && tc.builder.count() == 1

	if tc.splitter.CrossedBoundary() && !degenerate {
		err := tc.handleChunkBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (tc *chunker[S]) appendToParent(ctx context.Context, novel novelNode) (bool, error) {
	if tc.parent == nil {
		if err := tc.createParentChunker(ctx); err != nil {
			return false, err
		}
	}

	return tc.parent.append(ctx, novel.lastKey, novel.addr[:], novel.treeCount)
}

func (tc *chunker[S]) handleChunkBoundary(ctx context.Context) error {
	assertTrue(tc.builder.count() > 0, "in-progress chunk must be non-empty to create chunk boundary")

	novel, err := writeNewNode(ctx, tc.ns, tc.builder)
	if err != nil {
		return err
	}

	if _, err = tc.appendToParent(ctx, novel); err != nil {
		return err
	}

	tc.splitter.Reset()

	return nil
}

func (tc *chunker[S]) createParentChunker(ctx context.Context) (err error) {
	assertTrue(tc.parent == nil, "chunker parent must be nil")

	var parent *Cursor
	if tc.cur != nil && tc.cur.parent != nil {
		// todo(andy): does this comment make sense? cloning a pointer?
		// Clone the parent cursor because otherwise calling cur.forward() will affect our parent - and vice versa -
		// in surprising ways. Instead, Skip moves forward our parent's cursor if we forward across a boundary.
		parent = tc.cur.parent
	}

	tc.parent, err = newChunker(ctx, parent, tc.level+1, tc.ns, tc.serializer)
	if err != nil {
		return err
	}

	return nil
}

// Done returns the root Node of the resulting tree.
// The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (tc *chunker[S]) Done(ctx context.Context) (Node, error) {
	assertTrue(!tc.done, "chunker must not be done")
	tc.done = true

	if tc.cur != nil {
		if err := tc.finalizeCursor(ctx); err != nil {
			return Node{}, err
		}
	}

	// There is pending content above us, so we must push any remaining items from this Level up and allow some parent
	// to find the root of the resulting tree.
	if tc.parent != nil && tc.parent.anyPending() {
		if tc.builder.count() > 0 {
			// |tc.keys| are the last items at this Level of the tree,
			// make a chunk out of them
			if err := tc.handleChunkBoundary(ctx); err != nil {
				return Node{}, err
			}
		}

		return tc.parent.Done(ctx)
	}

	// At this point, we know |tc.keys| contains every item at this Level of the tree.
	// To see this, consider that there are two ways items can enter |tc.keys|.
	//  (1) as the result of processPrefix() with the cursor on anything other than the first item in the Node
	//  (2) as a result of a child chunker hitting an explicit chunk boundary during either Append() or finalize().
	//
	// The only way there can be no items in some parent chunker's |tc.keys| is if this chunker began with
	// a cursor within its first existing chunk (and thus all parents processPrefix()'d with a cursor on their first item) and
	// continued through all subsequent items without creating any explicit chunk boundaries (and thus never sent any
	// items up to a parent as a result of chunking). Therefore, this chunker's |tc.keys| must contain all items
	// within the current Node.

	// This Level must represent *a* root of the tree, but it is possibly non-canonical. There are three possible cases:
	// (1) This is "leaf" chunker and thus produced tree of depth 1 which contains exactly one chunk
	//     (never hit a boundary), or
	// (2) This in an internal Node of the tree which contains multiple references to child nodes. In either case,
	//     this is the canonical root of the tree.
	if tc.isLeaf() || tc.builder.count() > 1 {
		novel, err := writeNewNode(ctx, tc.ns, tc.builder)
		return novel.node, err
	}
	// (3) This is an internal Node of the tree with a single novelNode. This is a non-canonical root, and we must walk
	//     down until we find cases (1) or (2), above.
	assertTrue(!tc.isLeaf(), "chunker must not be leaf chunker")
	return getCanonicalRoot(ctx, tc.ns, tc.builder)
}

// If we are mutating an existing Node, appending subsequent items in the Node until we reach a pre-existing chunk
// boundary or the end of the Node.
func (tc *chunker[S]) finalizeCursor(ctx context.Context) (err error) {
	for tc.cur.Valid() {
		var sz uint64
		sz, err = tc.cur.currentSubtreeSize()
		if err != nil {
			return
		}
		var ok bool
		ok, err = tc.append(ctx,
			tc.cur.CurrentKey(),
			tc.cur.CurrentValue(),
			sz)
		if err != nil {
			return err
		}
		if ok && tc.cur.atNodeEnd() {
			break // boundary occurred at same place in old & new Node
		}

		err = tc.cur.Advance(ctx)
		if err != nil {
			return err
		}
	}

	if tc.cur.parent != nil {
		err := tc.cur.parent.Advance(ctx)

		if err != nil {
			return err
		}

		// Invalidate this cursor to mark it finalized.
		tc.cur.nd = Node{}
	}

	return nil
}

// Returns true if this nodeSplitter or any of its parents have any pending items in their |currentPair| slice.
func (tc *chunker[S]) anyPending() bool {
	if tc.builder.count() > 0 {
		return true
	}

	if tc.parent != nil {
		return tc.parent.anyPending()
	}

	return false
}

func (tc *chunker[S]) isLeaf() bool {
	return tc.level == 0
}

func getCanonicalRoot[S message.Serializer](ctx context.Context, ns NodeStore, builder *nodeBuilder[S]) (Node, error) {
	cnt := builder.count()
	assertTrue(cnt == 1, "in-progress chunk must be non-canonical to call getCanonicalRoot")

	nd, err := builder.build()
	if err != nil {
		return Node{}, err
	}
	mt := nd.getAddress(0)

	for {
		child, err := fetchChild(ctx, ns, mt)
		if err != nil {
			return Node{}, err
		}

		if child.IsLeaf() || child.count > 1 {
			return child, nil
		}

		mt = child.getAddress(0)
	}
}
