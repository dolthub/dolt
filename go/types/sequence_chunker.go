// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type boundaryChecker interface {
	// Write takes an item and returns true if the sequence should chunk after this item, false if not.
	Write(sequenceItem) bool
	// WindowSize returns the minimum number of items in a stream that must be written before resuming a chunking sequence.
	WindowSize() int
}

type newBoundaryCheckerFn func() boundaryChecker

type sequenceChunker struct {
	cur                        *sequenceCursor
	parent                     *sequenceChunker
	current                    []sequenceItem
	lastSeq                    sequence
	makeChunk, parentMakeChunk makeChunkFn
	boundaryChk                boundaryChecker
	newBoundaryChecker         newBoundaryCheckerFn
	done                       bool
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (metaTuple, sequence)

func newEmptySequenceChunker(makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	return newSequenceChunker(nil, makeChunk, parentMakeChunk, boundaryChk, newBoundaryChecker)
}

func newSequenceChunker(cur *sequenceCursor, makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.
	d.Chk.True(makeChunk != nil)
	d.Chk.True(parentMakeChunk != nil)
	d.Chk.True(boundaryChk != nil)
	d.Chk.True(newBoundaryChecker != nil)

	sc := &sequenceChunker{
		cur,
		nil,
		[]sequenceItem{},
		nil,
		makeChunk, parentMakeChunk,
		boundaryChk,
		newBoundaryChecker,
		false,
	}

	if cur != nil {
		if cur.parent != nil {
			sc.createParent()
		}

		// Number of previous items which must be hashed into the boundary checker.
		primeHashCount := boundaryChk.WindowSize() - 1

		// If the cursor is beyond the final position in the sequence, the preceeding item may have been a chunk boundary. In that case, we must test at least the preceeding item.
		appendPenultimate := cur.idx == cur.length()
		if appendPenultimate {
			// In that case, we prime enough items *prior* to the penultimate item to be correct.
			primeHashCount++
		}

		// Number of items preceeding initial cursor in present chunk.
		primeCurrentCount := cur.indexInChunk()

		// Number of items to fetch prior to cursor position
		prevCount := primeHashCount
		if primeCurrentCount > prevCount {
			prevCount = primeCurrentCount
		}

		prev := cur.maxNPrevItems(prevCount)
		for i, item := range prev {
			backIdx := len(prev) - i
			if appendPenultimate && backIdx == 1 {
				// Test the penultimate item for a boundary.
				sc.Append(item)
				continue
			}

			if backIdx <= primeHashCount {
				boundaryChk.Write(item)
			}

			if backIdx <= primeCurrentCount {
				sc.current = append(sc.current, item)
			}
		}
	}

	return sc
}

func (sc *sequenceChunker) Append(item sequenceItem) {
	d.Chk.True(item != nil)
	sc.current = append(sc.current, item)
	if sc.boundaryChk.Write(item) {
		sc.handleChunkBoundary(true)
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
	d.Chk.True(sc.parent == nil)
	var parent *sequenceCursor
	if sc.cur != nil && sc.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip moves forward our parent's cursor if we advance across a boundary.
		parent = sc.cur.parent.clone()
	}
	sc.parent = newSequenceChunker(parent, sc.parentMakeChunk, sc.parentMakeChunk, sc.newBoundaryChecker(), sc.newBoundaryChecker)
}

func (sc *sequenceChunker) handleChunkBoundary(createParentIfNil bool) {
	d.Chk.NotEmpty(sc.current)
	chunk, seq := sc.makeChunk(sc.current)
	sc.current = []sequenceItem{}
	sc.lastSeq = seq
	if sc.parent == nil && createParentIfNil {
		sc.createParent()
	}
	if sc.parent != nil {
		sc.parent.Append(chunk)
	}
}

func (sc *sequenceChunker) Done() sequence {
	d.Chk.False(sc.done)
	sc.done = true

	for s := sc; s != nil; s = s.parent {
		if s.cur != nil {
			s.finalizeCursor()
		}
	}

	// Chunkers will probably have current items which didn't hit a chunk boundary. Pretend they end on chunk boundaries for now.
	for s := sc; s != nil; s = s.parent {
		if len(s.current) > 0 {
			// Don't create a new parent if we haven't chunked.
			s.handleChunkBoundary(s.lastSeq != nil)
		}
	}

	// The rest of this code figures out which sequence in the parent chain is canonical. That is:
	// * It's empty, or
	// * It never chunked, so it's not a prollytree, or
	// * It chunked, so it's a prollytree, but it must have at least 2 children (or it could have been represented as that 1 child).
	//
	// Examples of when we may have constructed non-canonical sequences:
	// * If the previous tree (i.e. its cursor) was deeper, we will have created empty parents.
	// * If the last appended item was on a chunk boundary, there may be a sequence with a single chunk.

	// Firstly, follow up the parent chain to find the highest chunker which did chunk.
	var seq sequence
	for s := sc; s != nil; s = s.parent {
		if s.lastSeq != nil {
			seq = s.lastSeq
		}
	}

	if seq == nil {
		_, seq = sc.makeChunk([]sequenceItem{})
		return seq
	}

	// Lastly, step back down to find a meta sequence with more than 1 child.
	for seq.seqLen() <= 1 {
		d.Chk.NotEqual(0, seq.seqLen())
		ms, ok := seq.(metaSequence)
		if !ok {
			break
		}
		seq = ms.getChildSequence(0)
	}

	return seq
}

func (sc *sequenceChunker) finalizeCursor() {
	if !sc.cur.valid() {
		// The cursor is past the end, and due to the way cursors work, the parent cursor will actually point to its last chunk. We need to force it to point past the end so that our parent's Done() method doesn't add the last chunk twice.
		sc.skipParentIfExists()
		return
	}

	// Append the rest of the values in the sequence, up to the window size, plus the rest of that chunk. It needs to be the full window size because anything that was appended/skipped between chunker construction and finalization will have changed the hash state.
	fzr := sc.cur.clone()
	for i := 0; i < sc.boundaryChk.WindowSize() || fzr.indexInChunk() > 0; i++ {
		if i == 0 || fzr.indexInChunk() == 0 {
			// Every time we step into a chunk from the original sequence, that chunk will no longer exist in the new sequence. The parent must be instructed to skip it.
			sc.skipParentIfExists()
		}
		sc.Append(fzr.current())
		if !fzr.advance() {
			break
		}
	}
}
