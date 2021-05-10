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

package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/d"
)

type hashValueBytesFn func(item sequenceItem, rv *rollingValueHasher) error

type sequenceChunker struct {
	cur                        *sequenceCursor
	level                      uint64
	vrw                        ValueReadWriter
	parent                     *sequenceChunker
	current                    []sequenceItem
	makeChunk, parentMakeChunk makeChunkFn
	isLeaf                     bool
	hashValueBytes             hashValueBytesFn
	rv                         *rollingValueHasher
	done                       bool
	unwrittenCol               Collection
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(level uint64, values []sequenceItem) (Collection, orderedKey, uint64, error)

func newEmptySequenceChunker(ctx context.Context, vrw ValueReadWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) (*sequenceChunker, error) {
	return newSequenceChunker(ctx, nil, uint64(0), vrw, makeChunk, parentMakeChunk, hashValueBytes)
}

func newSequenceChunker(ctx context.Context, cur *sequenceCursor, level uint64, vrw ValueReadWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) (*sequenceChunker, error) {
	d.PanicIfFalse(makeChunk != nil)
	d.PanicIfFalse(parentMakeChunk != nil)
	d.PanicIfFalse(hashValueBytes != nil)
	d.PanicIfTrue(vrw == nil)

	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.

	sc := &sequenceChunker{
		cur,
		level,
		vrw,
		nil,
		make([]sequenceItem, 0, 1<<10),
		makeChunk, parentMakeChunk,
		true,
		hashValueBytes,
		newRollingValueHasher(vrw.Format(), byte(level%256)),
		false,
		nil,
	}

	if cur != nil {
		err := sc.resume(ctx)

		if err != nil {
			return nil, err
		}
	}

	return sc, nil
}

func (sc *sequenceChunker) resume(ctx context.Context) error {
	if sc.cur.parent != nil && sc.parent == nil {
		err := sc.createParent(ctx)

		if err != nil {
			return err
		}
	}

	idx := sc.cur.idx

	// Walk backwards to the start of the existing chunk.
	for sc.cur.indexInChunk() > 0 {
		ok, err := sc.cur.retreatMaybeAllowBeforeStart(ctx, false)

		if err != nil {
			return err
		}

		if !ok {
			break
		}
	}

	for sc.cur.idx < idx {
		item, err := sc.cur.current()

		if err != nil {
			return err
		}

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

// advanceTo advances the sequenceChunker to the next "spine" at which
// modifications to the prolly-tree should take place
func (sc *sequenceChunker) advanceTo(ctx context.Context, next *sequenceCursor) error {
	// There are four basic situations which must be handled when advancing to a
	// new chunking position:
	//
	// Case (1): |sc.cur| and |next| are exactly aligned. In this case, there's
	//           nothing to do. Just assign sc.cur = next.
	//
	// Case (2): |sc.cur| is "ahead" of |next|. This can only have resulted from
	//           advancing of a lower level causing |sc.cur| to advance. In this
	//           case, we advance |next| until the cursors are aligned and then
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
		item, err := sc.cur.current()

		if err != nil {
			return err
		}

		if ok, err := sc.Append(ctx, item); err != nil {
			return err
		} else if ok && sc.cur.atLastItem() {
			if sc.cur.parent != nil {

				if sc.cur.parent.compare(next.parent) < 0 {
					// Case (4): We stopped consuming items on this level before entering
					// the sequence referenced by |next|
					reachedNext = false
				}

				// Note: Logically, what is happening here is that we are consuming the
				// item at the current level. Logically, we'd call sc.cur.advance(),
				// but that would force loading of the next sequence, which we don't
				// need for any reason, so instead we advance the parent and take care
				// not to allow it to step outside the sequence.
				_, err := sc.cur.parent.advanceMaybeAllowPastEnd(ctx, false)

				if err != nil {
					return err
				}

				// Invalidate this cursor, since it is now inconsistent with its parent
				sc.cur.parent = nil
				sc.cur.seq = nil
			}

			break
		}

		_, err = sc.cur.advance(ctx)

		if err != nil {
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

func (sc *sequenceChunker) Append(ctx context.Context, item sequenceItem) (bool, error) {
	d.PanicIfTrue(item == nil)
	sc.current = append(sc.current, item)
	err := sc.hashValueBytes(item, sc.rv)

	if err != nil {
		return false, err
	}

	if sc.rv.crossedBoundary {
		// When a metaTuple contains a key that is so large that it causes a chunk boundary to be crossed simply by encoding
		// the metaTuple then we will create a metaTuple to encode the metaTuple containing the same key again, and again
		// crossing a chunk boundary causes infinite recursion.  The solution is not to allow a metaTuple with a single
		// leaf to cross chunk boundaries.
		isOneLeafedMetaTuple := false
		if mt, ok := item.(metaTuple); ok {
			isOneLeafedMetaTuple = mt.numLeaves() == 1
		}

		if !isOneLeafedMetaTuple {
			err := sc.handleChunkBoundary(ctx)

			if err != nil {
				return false, err
			}

			return true, nil
		}
	}
	return false, nil
}

func (sc *sequenceChunker) Skip(ctx context.Context) error {
	_, err := sc.cur.advance(ctx)

	if err != nil {
		return err
	}

	return nil
}

func (sc *sequenceChunker) createParent(ctx context.Context) error {
	d.PanicIfFalse(sc.parent == nil)
	var parent *sequenceCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip moves forward our parent's cursor if we advance across a boundary.
		parent = sc.cur.parent
	}

	var err error
	sc.parent, err = newSequenceChunker(ctx, parent, sc.level+1, sc.vrw, sc.parentMakeChunk, sc.parentMakeChunk, metaHashValueBytes)

	if err != nil {
		return err
	}

	sc.parent.isLeaf = false

	if sc.unwrittenCol != nil {
		// There is an unwritten collection, but this chunker now has a parent, so
		// write it. See createSequence().
		_, err := sc.vrw.WriteValue(ctx, sc.unwrittenCol)

		if err != nil {
			return err
		}

		sc.unwrittenCol = nil
	}

	return nil
}

// createSequence creates a sequence from the current items in |sc.current|,
// clears the current items, then returns the new sequence and a metaTuple that
// points to it.
//
// If |write| is true then the sequence is eagerly written, or if false it's
// manually constructed and stored in |sc.unwrittenCol| to possibly write later
// in createParent(). This is to hopefully avoid unnecessarily writing the root
// chunk (for example, the sequence may be stored inline).
//
// There is a catch: in the rare case that the root chunk is actually not the
// canonical root of the sequence (see Done()), then we will have ended up
// unnecessarily writing a chunk - the canonical root. However, this is a fair
// tradeoff for simplicity of the chunking algorithm.
func (sc *sequenceChunker) createSequence(ctx context.Context, write bool) (sequence, metaTuple, error) {
	col, key, numLeaves, err := sc.makeChunk(sc.level, sc.current)

	if err != nil {
		return nil, metaTuple{}, err
	}

	// |sc.makeChunk| copies |sc.current| so it's safe to re-use the memory.
	sc.current = sc.current[:0]

	var ref Ref
	if write {
		ref, err = sc.vrw.WriteValue(ctx, col)
	} else {
		ref, err = NewRef(col, sc.vrw.Format())
		sc.unwrittenCol = col
	}

	if err != nil {
		return nil, metaTuple{}, err
	}

	mt, err := newMetaTuple(ref, key, numLeaves)

	if err != nil {
		return nil, metaTuple{}, err
	}

	return col.asSequence(), mt, nil
}

func (sc *sequenceChunker) handleChunkBoundary(ctx context.Context) error {
	d.PanicIfFalse(len(sc.current) > 0)
	sc.rv.Reset()
	if sc.parent == nil {
		err := sc.createParent(ctx)

		if err != nil {
			return err
		}
	}
	_, mt, err := sc.createSequence(ctx, true)

	if err != nil {
		return err
	}

	_, err = sc.parent.Append(ctx, mt)

	if err != nil {
		return err
	}

	return nil
}

// Returns true if this chunker or any of its parents have any pending items in their |current| slice.
func (sc *sequenceChunker) anyPending() bool {
	if len(sc.current) > 0 {
		return true
	}

	if sc.parent != nil {
		return sc.parent.anyPending()
	}

	return false
}

// Returns the root sequence of the resulting tree. The logic here is subtle, but hopefully correct and understandable. See comments inline.
func (sc *sequenceChunker) Done(ctx context.Context) (sequence, error) {
	d.PanicIfTrue(sc.done)
	sc.done = true

	if sc.cur != nil {
		err := sc.finalizeCursor(ctx)

		if err != nil {
			return nil, err
		}
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent to find the root of the resulting tree.
	if sc.parent != nil && sc.parent.anyPending() {
		if len(sc.current) > 0 {
			// If there are items in |current| at this point, they represent the final items of the sequence which occurred beyond the previous *explicit* chunk boundary. The end of input of a sequence is considered an *implicit* boundary.
			err := sc.handleChunkBoundary(ctx)

			if err != nil {
				return nil, err
			}
		}

		return sc.parent.Done(ctx)
	}

	// At this point, we know this chunker contains, in |current| every item at this level of the resulting tree. To see this, consider that there are two ways a chunker can enter items into its |current|: (1) as the result of resume() with the cursor on anything other than the first item in the sequence, and (2) as a result of a child chunker hitting an explicit chunk boundary during either Append() or finalize(). The only way there can be no items in some parent chunker's |current| is if this chunker began with cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any items up to a parent as a result of chunking). Therefore, this chunker's current must contain all items within the current sequence.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three cases to consider:

	// (1) This is "leaf" chunker and thus produced tree of depth 1 which contains exactly one chunk (never hit a boundary), or (2) This in an internal node of the tree which contains multiple references to child nodes. In either case, this is the canonical root of the tree.
	if sc.isLeaf || len(sc.current) > 1 {
		seq, _, err := sc.createSequence(ctx, false)

		if err != nil {
			return nil, err
		}

		return seq, nil
	}

	// (3) This is an internal node of the tree which contains a single reference to a child node. This can occur if a non-leaf chunker happens to chunk on the first item (metaTuple) appended. In this case, this is the root of the tree, but it is *not* canonical and we must walk down until we find cases (1) or (2), above.
	d.PanicIfFalse(!sc.isLeaf && len(sc.current) == 1)
	mt := sc.current[0].(metaTuple)

	for {
		child, err := mt.getChildSequence(ctx, sc.vrw)

		if err != nil {
			return nil, err
		}

		if _, ok := child.(metaSequence); !ok || child.seqLen() > 1 {
			return child, nil
		}

		item, err := child.getItem(0)

		if err != nil {
			return nil, err
		}

		mt = item.(metaTuple)
	}
}

// If we are mutating an existing sequence, appending subsequent items in the sequence until we reach a pre-existing chunk boundary or the end of the sequence.
func (sc *sequenceChunker) finalizeCursor(ctx context.Context) error {
	for sc.cur.valid() {
		item, err := sc.cur.current()

		if err != nil {
			return err
		}

		if ok, err := sc.Append(ctx, item); err != nil {
			return err
		} else if ok && sc.cur.atLastItem() {
			break // boundary occurred at same place in old & new sequence
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
		sc.cur.seq = nil
	}

	return nil
}
