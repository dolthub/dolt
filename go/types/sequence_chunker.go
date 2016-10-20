// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type hashValueBytesFn func(item sequenceItem, rv *rollingValueHasher)

type sequenceChunker struct {
	cur                        *sequenceCursor
	vr                         ValueReader
	vw                         ValueWriter
	parent                     *sequenceChunker
	current                    []sequenceItem
	makeChunk, parentMakeChunk makeChunkFn
	isLeaf                     bool
	hashValueBytes             hashValueBytesFn
	rv                         *rollingValueHasher
	done                       bool
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (Collection, orderedKey, uint64)

func newEmptySequenceChunker(vr ValueReader, vw ValueWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) *sequenceChunker {
	return newSequenceChunker(nil, vr, vw, makeChunk, parentMakeChunk, hashValueBytes)
}

func newSequenceChunker(cur *sequenceCursor, vr ValueReader, vw ValueWriter, makeChunk, parentMakeChunk makeChunkFn, hashValueBytes hashValueBytesFn) *sequenceChunker {
	d.PanicIfFalse(makeChunk != nil)
	d.PanicIfFalse(parentMakeChunk != nil)
	d.PanicIfFalse(hashValueBytes != nil)

	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.

	sc := &sequenceChunker{
		cur,
		vr,
		vw,
		nil,
		[]sequenceItem{},
		makeChunk, parentMakeChunk,
		true,
		hashValueBytes,
		newRollingValueHasher(),
		false,
	}

	if cur != nil {
		sc.resume()
	}

	return sc
}

func (sc *sequenceChunker) resume() {
	if sc.cur.parent != nil {
		sc.createParent()
	}

	// Number of previous items' value bytes which must be hashed into the boundary checker.
	primeHashBytes := int64(sc.rv.window)

	appendCount := 0
	primeHashCount := 0

	// If the cursor is beyond the final position in the sequence, then we can't tell the difference between it having been an explicit and implicit boundary. Since the caller may be about to append another value, we need to know whether the existing final item is an explicit chunk boundary.
	cursorBeyondFinal := sc.cur.idx == sc.cur.length()
	if cursorBeyondFinal && sc.cur.retreatMaybeAllowBeforeStart(false) {
		// In that case, we prime enough items *prior* to the final item to be correct.
		appendCount++
		primeHashCount++
	}

	// Walk backwards to the start of the existing chunk.
	sc.rv.lengthOnly = true
	for sc.cur.indexInChunk() > 0 && sc.cur.retreatMaybeAllowBeforeStart(false) {
		appendCount++
		if primeHashBytes > 0 {
			primeHashCount++
			sc.rv.ClearLastBoundary()
			sc.hashValueBytes(sc.cur.current(), sc.rv)
			primeHashBytes -= int64(sc.rv.bytesHashed)
		}
	}

	// If the hash window won't be filled by the preceding items in the current chunk, walk further back until they will.
	for primeHashBytes > 0 && sc.cur.retreatMaybeAllowBeforeStart(false) {
		primeHashCount++
		sc.rv.ClearLastBoundary()
		sc.hashValueBytes(sc.cur.current(), sc.rv)
		primeHashBytes -= int64(sc.rv.bytesHashed)
	}
	sc.rv.lengthOnly = false

	for primeHashCount > 0 || appendCount > 0 {
		item := sc.cur.current()
		sc.cur.advance()

		if primeHashCount > appendCount {
			// Before the start of the current chunk: just hash value bytes into window
			sc.hashValueBytes(item, sc.rv)
			primeHashCount--
			continue
		}

		if appendCount > primeHashCount {
			// In current chunk, but before window: just append item.
			sc.current = append(sc.current, item)
			appendCount--
			continue
		}

		sc.rv.ClearLastBoundary()
		sc.hashValueBytes(item, sc.rv)
		sc.current = append(sc.current, item)

		// Within current chunk and hash window: append item & hash value bytes into window.
		if sc.rv.crossedBoundary && cursorBeyondFinal && appendCount == 1 {
			// The cursor is positioned immediately after the final item in the sequence and it *was* an *explicit* chunk boundary: create a chunk.
			sc.handleChunkBoundary()
		}

		appendCount--
		primeHashCount--
	}
}

func (sc *sequenceChunker) Append(item sequenceItem) {
	d.PanicIfFalse(item != nil)
	sc.current = append(sc.current, item)
	sc.rv.ClearLastBoundary()
	sc.hashValueBytes(item, sc.rv)
	if sc.rv.crossedBoundary {
		sc.handleChunkBoundary()
	}
}

func (sc *sequenceChunker) Skip() {
	if sc.cur.advance() && sc.cur.indexInChunk() == 0 {
		// Advancing moved our cursor into the next chunk. We need to advance our parent's cursor, so that when our parent writes out the remaining chunks it doesn't include the chunk that we skipped.
		sc.skipParentIfExists()
	}
}

func (sc *sequenceChunker) skipParentIfExists() {
	if sc.parent != nil && sc.parent.cur != nil {
		sc.parent.Skip()
	}
}

func (sc *sequenceChunker) createParent() {
	d.PanicIfFalse(sc.parent == nil)
	var parent *sequenceCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip moves forward our parent's cursor if we advance across a boundary.
		parent = sc.cur.parent.clone()
	}
	sc.parent = newSequenceChunker(parent, sc.vr, sc.vw, sc.parentMakeChunk, sc.parentMakeChunk, metaHashValueBytes)
	sc.parent.isLeaf = false
}

func (sc *sequenceChunker) createSequence() (sequence, metaTuple) {
	// If the sequence chunker has a ValueWriter, eagerly write sequences.
	col, key, numLeaves := sc.makeChunk(sc.current)
	seq := col.sequence()
	var ref Ref
	if sc.vw != nil {
		ref = sc.vw.WriteValue(col)
		col = nil
	} else {
		ref = NewRef(col)
	}
	mt := newMetaTuple(ref, key, numLeaves, col)

	sc.current = []sequenceItem{}
	return seq, mt
}

func (sc *sequenceChunker) handleChunkBoundary() {
	d.Chk.NotEmpty(sc.current)

	_, mt := sc.createSequence()
	if sc.parent == nil {
		sc.createParent()
	}
	sc.parent.Append(mt)
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
func (sc *sequenceChunker) Done() sequence {
	d.PanicIfTrue(sc.done)
	sc.done = true

	if sc.cur != nil {
		sc.finalizeCursor()
	}

	// There is pending content above us, so we must push any remaining items from this level up and allow some parent to find the root of the resulting tree.
	if sc.parent != nil && sc.parent.anyPending() {
		if len(sc.current) > 0 {
			// If there are items in |current| at this point, they represent the final items of the sequence which occurred beyond the previous *explicit* chunk boundary. The end of input of a sequence is considered an *implicit* boundary.
			sc.handleChunkBoundary()
		}

		return sc.parent.Done()
	}

	// At this point, we know this chunker contains, in |current| every item at this level of the resulting tree. To see this, consider that there are two ways a chunker can enter items into its |current|: (1) as the result of resume() with the cursor on anything other than the first item in the sequence, and (2) as a result of a child chunker hitting an explicit chunk boundary during either Append() or finalize(). The only way there can be no items in some parent chunker's |current| is if this chunker began with cursor within its first existing chunk (and thus all parents resume()'d with a cursor on their first item) and continued through all sebsequent items without creating any explicit chunk boundaries (and thus never sent any items up to a parent as a result of chunking). Therefore, this chunker's current must contain all items within the current sequence.

	// This level must represent *a* root of the tree, but it is possibly non-canonical. There are three cases to consider:

	// (1) This is "leaf" chunker and thus produced tree of depth 1 which contains exactly one chunk (never hit a boundary), or (2) This in an internal node of the tree which contains multiple references to child nodes. In either case, this is the canonical root of the tree.
	if sc.isLeaf || len(sc.current) > 1 {
		seq, _ := sc.createSequence()
		return seq
	}

	// (3) This is an internal node of the tree which contains a single reference to a child node. This can occur if a non-leaf chunker happens to chunk on the first item (metaTuple) appended. In this case, this is the root of the tree, but it is *not* canonical and we must walk down until we find cases (1) or (2), above.
	d.PanicIfFalse(!sc.isLeaf && len(sc.current) == 1)
	mt := sc.current[0].(metaTuple)

	for {
		child := mt.getChildSequence(sc.vr)
		if _, ok := child.(metaSequence); !ok || child.seqLen() > 1 {
			return child
		}

		mt = child.getItem(0).(metaTuple)
	}
}

// If we are mutating an existing sequence, appending subsequent items in the sequence until we reach a pre-existing chunk boundary or the end of the sequence.
func (sc *sequenceChunker) finalizeCursor() {
	if !sc.cur.valid() {
		// The cursor is past the end, and due to the way cursors work, the parent cursor will actually point to its last chunk. We need to force it to point past the end so that our parent's Done() method doesn't add the last chunk twice.
		sc.skipParentIfExists()
		return
	}

	// Append the rest of the values in the sequence, up to the window size, plus the rest of that chunk. It needs to be the full window size because anything that was appended/skipped between chunker construction and finalization will have changed the hash state.
	hashWindow := int64(sc.rv.window)

	isBoundary := len(sc.current) == 0

	// We can terminate when: (1) we hit the end input in this sequence or (2) we process beyond the hash window and encounter an item which is boundary in both the old and new state of the sequence.
	for i := 0; sc.cur.valid() && (hashWindow > 0 || sc.cur.indexInChunk() > 0 || !isBoundary); i++ {
		if i == 0 || sc.cur.indexInChunk() == 0 {
			// Every time we step into a chunk from the original sequence, that chunk will no longer exist in the new sequence. The parent must be instructed to skip it.
			sc.skipParentIfExists()
		}

		item := sc.cur.current()
		sc.current = append(sc.current, item)
		isBoundary = false

		sc.cur.advance()

		if hashWindow > 0 {
			// While we are within the hash window, we need to continue to hash items into the rolling hash and explicitly check for resulting boundaries.
			sc.rv.ClearLastBoundary()
			sc.hashValueBytes(item, sc.rv)
			hashWindow -= int64(sc.rv.bytesHashed)
			isBoundary = sc.rv.crossedBoundary
		} else if sc.cur.indexInChunk() == 0 {
			// Once we are beyond the hash window, we know that boundaries can only occur in the same place they did within the existing sequence.
			isBoundary = true
		}

		if isBoundary {
			sc.handleChunkBoundary()
		}
	}
}
