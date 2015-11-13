package types

import "github.com/attic-labs/noms/d"

type sequenceItem interface{}

type sequenceChunker struct {
	parent                       *sequenceChunker
	current, pendingFirst        []sequenceItem
	makeChunk, parentMakeChunk   makeChunkFn
	isBoundary, parentIsBoundary isBoundaryFn
}

// makeChunkFn takes a sequence of items to chunk, and returns the result of chunking those items, a tuple of a reference to that chunk which can itself be chunked + its underlying value.
type makeChunkFn func(values []sequenceItem) (sequenceItem, interface{})

// isBoundaryFn takes an item and returns true if the sequence should chunk after this item, false if not.
type isBoundaryFn func(value sequenceItem) bool

func newSequenceChunker(makeChunk, parentMakeChunk makeChunkFn, isBoundary, parentIsBoundary isBoundaryFn) *sequenceChunker {
	d.Chk.NotNil(makeChunk)
	d.Chk.NotNil(parentMakeChunk)
	d.Chk.NotNil(isBoundary)
	d.Chk.NotNil(parentIsBoundary)
	return &sequenceChunker{
		nil,
		[]sequenceItem{}, nil,
		makeChunk, parentMakeChunk,
		isBoundary, parentIsBoundary,
	}
}

func (seq *sequenceChunker) Append(item sequenceItem) {
	// Checking for seq.pendingFirst must happen immediately, because it's effectively a continuation from the last call to Append. Specifically, if the last call to Append created the first chunk boundary, delay creating the parent until absolutely necessary. Otherwise, we will be in a state where a parent has only a single item, which is invalid.
	if seq.pendingFirst != nil {
		d.Chk.True(seq.parent == nil)
		seq.parent = newSequenceChunker(seq.parentMakeChunk, seq.parentMakeChunk, seq.parentIsBoundary, seq.parentIsBoundary)
		chunk, _ := seq.makeChunk(seq.pendingFirst)
		seq.parent.Append(chunk)
		seq.pendingFirst = nil
	}
	seq.current = append(seq.current, item)
	if seq.isBoundary(item) {
		seq.handleChunkBoundary()
	}
}

func (seq *sequenceChunker) handleChunkBoundary() {
	if seq.parent == nil {
		seq.pendingFirst = seq.current
	} else {
		chunk, _ := seq.makeChunk(seq.current)
		seq.parent.Append(chunk)
	}
	seq.current = []sequenceItem{}
}

func (seq *sequenceChunker) Done() (sequenceItem, interface{}) {
	if seq.pendingFirst != nil {
		d.Chk.True(seq.parent == nil)
		return seq.makeChunk(seq.pendingFirst)
	}
	if seq.parent != nil {
		if len(seq.current) > 0 {
			seq.handleChunkBoundary()
		}
		return seq.parent.Done()
	}
	return seq.makeChunk(seq.current)
}
