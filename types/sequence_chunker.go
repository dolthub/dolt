package types

import "github.com/attic-labs/noms/d"

type sequenceItem interface{}

type boundaryChecker interface {
	// Write takes an item and returns true if the sequence should chunk after this item, false if not.
	Write(sequenceItem) bool
	// WindowSize returns the minimum number of items in a stream that must be written before resuming a chunking sequence.
	WindowSize() int
}

type newBoundaryCheckerFn func() boundaryChecker

type sequenceChunker struct {
	cur                        *sequenceCursor
	isOnChunkBoundary          bool
	parent                     *sequenceChunker
	current                    []sequenceItem
	makeChunk, parentMakeChunk makeChunkFn
	boundaryChk                boundaryChecker
	newBoundaryChecker         newBoundaryCheckerFn
	used                       bool
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (sequenceItem, Value)

func newEmptySequenceChunker(makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	return newSequenceChunker(nil, makeChunk, parentMakeChunk, boundaryChk, newBoundaryChecker)
}

func newSequenceChunker(cur *sequenceCursor, makeChunk, parentMakeChunk makeChunkFn, boundaryChk boundaryChecker, newBoundaryChecker newBoundaryCheckerFn) *sequenceChunker {
	// |cur| will be nil if this is a new sequence, implying this is a new tree, or the tree has grown in height relative to its original chunked form.
	d.Chk.NotNil(makeChunk)
	d.Chk.NotNil(parentMakeChunk)
	d.Chk.NotNil(boundaryChk)
	d.Chk.NotNil(newBoundaryChecker)

	seq := &sequenceChunker{
		cur,
		false,
		nil,
		[]sequenceItem{},
		makeChunk, parentMakeChunk,
		boundaryChk,
		newBoundaryChecker,
		false,
	}

	if cur != nil {
		// Eagerly create a chunker for each level of the existing tree, but note that we may not necessarily need them all, since chunk boundaries may change such that the tree ends up shallower. The |seq.used| flag accounts for that case.
		if cur.parent != nil {
			seq.createParent()
		}
		// Prime the chunker into the state it would be if all items in the sequence had been appended one at a time.
		// This can be WindowSize-1, not WindowSize, because the first appended item will fill the remaining spot in the hash window.
		for _, item := range cur.maxNPrevItems(boundaryChk.WindowSize() - 1) {
			boundaryChk.Write(item)
		}
		// Reconstruct this entire chunk.
		seq.current = cur.maxNPrevItems(cur.indexInChunk())
		seq.used = len(seq.current) > 0
	}

	return seq
}

func (seq *sequenceChunker) Append(item sequenceItem) {
	d.Chk.NotNil(item)
	// Check |isOnChunkBoundary| immediately, because it's effectively a continuation from the last call to Append. Specifically, this happens when the last call to Append created the first chunk boundary, which delayed creating the parent until absolutely necessary. Otherwise, we will be in a state where a parent has only a single item, which is invalid.
	if seq.isOnChunkBoundary {
		seq.createParent()
		seq.handleChunkBoundary()
		seq.isOnChunkBoundary = false
	}
	seq.current = append(seq.current, item)
	seq.used = true
	if seq.boundaryChk.Write(item) {
		seq.handleChunkBoundary()
	}
}

func (seq *sequenceChunker) Skip() {
	if seq.cur.advance() && seq.cur.indexInChunk() == 0 {
		// Advancing moved our cursor into the next chunk. We need to advance our parent's cursor, so that when our parent writes out the remaining chunks it doesn't include the chunk that we skipped.
		seq.skipParentIfExists()
	}
}

func (seq *sequenceChunker) skipParentIfExists() {
	if seq.parent != nil && seq.parent.cur != nil {
		seq.parent.Skip()
	}
}

func (seq *sequenceChunker) createParent() {
	d.Chk.Nil(seq.parent)
	var parent *sequenceCursor
	if seq.cur != nil && seq.cur.parent != nil {
		// Clone the parent cursor because otherwise calling cur.advance() will affect our parent - and vice versa - in surprising ways. Instead, Skip moves forward our parent's cursor if we advance across a boundary.
		parent = seq.cur.parent.clone()
	}
	seq.parent = newSequenceChunker(parent, seq.parentMakeChunk, seq.parentMakeChunk, seq.newBoundaryChecker(), seq.newBoundaryChecker)
}

func (seq *sequenceChunker) handleChunkBoundary() {
	d.Chk.NotEmpty(seq.current)
	if seq.parent == nil {
		// Wait until there is a parent.
		d.Chk.False(seq.isOnChunkBoundary)
		seq.isOnChunkBoundary = true
	} else {
		chunk, _ := seq.makeChunk(seq.current)
		seq.parent.Append(chunk)
		seq.current = []sequenceItem{}
	}
}

func (seq *sequenceChunker) Done() Value {
	if seq.cur != nil {
		seq.finalizeCursor()
	}

	if seq.isRoot() {
		_, done := seq.makeChunk(seq.current)
		return internalValueFromType(done, done.Type())
	}

	if len(seq.current) > 0 {
		seq.handleChunkBoundary()
	}
	return seq.parent.Done()
}

func (seq *sequenceChunker) isRoot() bool {
	for ancstr := seq.parent; ancstr != nil; ancstr = ancstr.parent {
		if ancstr.used {
			return false
		}
	}
	return true
}

func (seq *sequenceChunker) finalizeCursor() {
	if _, ok := seq.cur.maybeCurrent(); !ok {
		// The cursor is past the end, and due to the way cursors work, the parent cursor will actually point to its last chunk. We need to force it to point past the end so that our parent's Done() method doesn't add the last chunk twice.
		seq.skipParentIfExists()
		return
	}

	// Append the rest of the values in the sequence, up to the window size, plus the rest of that chunk. It needs to be the full window size because anything that was appended/skipped between chunker construction and finalization will have changed the hash state.
	fzr := seq.cur.clone()
	for i := 0; i < seq.boundaryChk.WindowSize() || fzr.indexInChunk() > 0; i++ {
		if i == 0 || fzr.indexInChunk() == 0 {
			// Every time we step into a chunk from the original sequence, that chunk will no longer exist in the new sequence. The parent must be instructed to skip it.
			seq.skipParentIfExists()
		}
		seq.Append(fzr.current())
		if !fzr.advance() {
			break
		}
	}
}
