// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// iterSequence efficiently iterates through a sequence calling cb for
// each item.
//
// This is preferred to sequenceCursor.iter()
func iterSequence(sc *sequenceCursor, cb cursorIterCallback) {
	sc.enableReadAhead()
	defer func() {
		sc.readAhead = nil
	}()
	it := &sequenceIterator{sc}
	for it.hasMore() && !cb(it.item()) {
		it.advance(1)
	}
}

// sequenceIterator iterates forward through a sequence.
//
// Since it can assume the sequence is being read forward, it reads
// upcoming chunks ahead of their access to optimize throughput
type sequenceIterator struct {
	cursor *sequenceCursor
}

// newSequenceIterator creates an iterator for reading a sequence
func newSequenceIterator(seq sequence, idx uint64) *sequenceIterator {
	sc := newCursorAtIndex(seq, idx)
	sc.enableReadAhead()
	return &sequenceIterator{sc}
}

// hasMore return true if there's more to iterate
func (si sequenceIterator) hasMore() bool {
	return si.cursor.valid()
}

// advance advances the iterator by n items
func (si sequenceIterator) advance(n int) bool {
	for i := 0; i < n && si.cursor.advance(); i++ {
	}
	return si.cursor.valid()
}

// item returns the value at the current position
func (si sequenceIterator) item() sequenceItem {
	return si.cursor.current()
}

// chunkAndIndex returns the current leaf chunk and the index in that chunk referring to Item()
func (si sequenceIterator) chunkAndIndex() (sequence, int) {
	return si.cursor.seq, si.cursor.idx
}

// hitRate returns the read-ahead cache hit rate
func (si sequenceIterator) readAheadHitRate() float32 {
	if ra := si.cursor.readAhead; ra != nil {
		return ra.hitRate()
	}
	return 0.0
}
