package types

import (
	"github.com/attic-labs/noms/chunks"
)

// sequenceChunkerCursor wraps a metaSequenceCursor to give it the ability to advance/retreat through individual items, not just meta nodes.
type sequenceChunkerCursor struct {
	parent    sequenceCursor
	leaf      []sequenceItem
	leafIdx   int
	readChunk readChunkFn
	cs        chunks.ChunkSource
}

// readChunkFn takes an item in the sequence which points to a chunk, and returns the sequence of items in that chunk.
type readChunkFn func(sequenceItem) []sequenceItem

func newSequenceChunkerCursor(ms *metaSequenceCursor, leaf []sequenceItem, leafIdx int, readChunk readChunkFn, cs chunks.ChunkSource) sequenceCursor {
	return &sequenceChunkerCursor{ms, leaf, leafIdx, readChunk, cs}
}

func (scc *sequenceChunkerCursor) current() sequenceItem {
	return scc.leaf[scc.leafIdx]
}

func (scc *sequenceChunkerCursor) indexInChunk() int {
	return scc.leafIdx
}

func (scc *sequenceChunkerCursor) advance() bool {
	scc.leafIdx++
	if scc.leafIdx < len(scc.leaf) {
		return true
	}
	if scc.parent.advance() {
		scc.leaf = scc.readChunk(scc.parent.current())
		scc.leafIdx = 0
		return true
	}
	return false
}

func (scc *sequenceChunkerCursor) retreat() bool {
	if scc.leafIdx > 0 {
		scc.leafIdx--
		return true
	}
	if scc.parent.retreat() {
		scc.leaf = scc.readChunk(scc.parent.current())
		scc.leafIdx = len(scc.leaf) - 1
		return true
	}
	return false
}

func (scc *sequenceChunkerCursor) prevItems(n int) (prev []sequenceItem) {
	retreater := scc.clone()
	for i := 0; i < n && retreater.retreat(); i++ {
		prev = append(prev, retreater.current())
	}
	for i := 0; i < len(prev)/2; i++ {
		t := prev[i]
		prev[i] = prev[len(prev)-i-1]
		prev[len(prev)-i-1] = t
	}
	return
}

func (scc *sequenceChunkerCursor) clone() sequenceCursor {
	var parent sequenceCursor
	if scc.parent != nil {
		parent = scc.parent.clone()
	}

	return &sequenceChunkerCursor{parent, scc.leaf, scc.leafIdx, scc.readChunk, scc.cs}
}

func (scc *sequenceChunkerCursor) getParent() sequenceCursor {
	return scc.parent
}
