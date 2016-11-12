// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"runtime"

	"github.com/attic-labs/noms/go/d"
)

// The number of go routines to devote to read-ahead.
// The current setting provides good throughput on on a
// 2015 MacBook Pro/2.7 GHz i5 /8 GB.
var readAheadParallelism = runtime.NumCPU() * 64

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent    *sequenceCursor
	seq       sequence
	idx       int
	readAhead *sequenceReadAhead
}

// newSequenceCursor creates a cursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newSequenceCursor(parent *sequenceCursor, seq sequence, idx int) *sequenceCursor {
	d.PanicIfTrue(seq == nil)
	if idx < 0 {
		idx += seq.seqLen()
		d.PanicIfFalse(idx >= 0)
	}
	return &sequenceCursor{parent, seq, idx, nil}
}

func (cur *sequenceCursor) length() int {
	return cur.seq.seqLen()
}

func (cur *sequenceCursor) getItem(idx int) sequenceItem {
	return cur.seq.getItem(idx)
}

// sync loads the sequence that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *sequenceCursor) sync() {
	d.PanicIfFalse(cur.parent != nil)
	if cur.readAhead != nil {
		v := cur.parent.current()
		hash := v.(metaTuple).ref.TargetHash()
		if cs, ok := cur.readAhead.get(cur.parent.idx, hash); ok {
			cur.seq = cs
			return
		}
	}
	cur.seq = cur.parent.getChildSequence()
}

// getChildSequence retrieves the child at the current cursor position.
func (cur *sequenceCursor) getChildSequence() sequence {
	return cur.seq.getChildSequence(cur.idx)
}

// current returns the value at the current cursor position
func (cur *sequenceCursor) current() sequenceItem {
	d.PanicIfFalse(cur.valid())
	return cur.getItem(cur.idx)
}

func (cur *sequenceCursor) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.length()
}

func (cur *sequenceCursor) depth() int {
	if nil != cur.parent {
		return 1 + cur.parent.depth()
	}
	return 1
}

func (cur *sequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *sequenceCursor) advance() bool {
	return cur.advanceMaybeAllowPastEnd(true)
}

func (cur *sequenceCursor) advanceMaybeAllowPastEnd(allowPastEnd bool) bool {
	if cur.idx < cur.length()-1 {
		cur.idx++
		return true
	}
	if cur.idx == cur.length() {
		return false
	}
	if cur.parent != nil && cur.parent.advanceMaybeAllowPastEnd(false) {
		// at end of current leaf chunk and there are more
		cur.sync()
		cur.idx = 0
		return true
	}
	if allowPastEnd {
		cur.idx++
	}
	return false
}

func (cur *sequenceCursor) retreat() bool {
	return cur.retreatMaybeAllowBeforeStart(true)
}

func (cur *sequenceCursor) retreatMaybeAllowBeforeStart(allowBeforeStart bool) bool {
	if cur.idx > 0 {
		cur.idx--
		return true
	}
	if cur.idx == -1 {
		return false
	}
	d.PanicIfFalse(0 == cur.idx)
	if cur.parent != nil && cur.parent.retreatMaybeAllowBeforeStart(false) {
		cur.sync()
		cur.idx = cur.length() - 1
		return true
	}
	if allowBeforeStart {
		cur.idx--
	}
	return false
}

// clone creates a copy of the cursor
func (cur *sequenceCursor) clone() *sequenceCursor {
	var parent *sequenceCursor
	if cur.parent != nil {
		parent = cur.parent.clone()
	}
	return newSequenceCursor(parent, cur.seq, cur.idx)
}

type cursorIterCallback func(item interface{}) bool

// iter iterates forward from the current position
// TODO: replace calls to this with direct calls to IterSequence()
func (cur *sequenceCursor) iter(cb cursorIterCallback) {
	iterSequence(cur, cb)
}

// newCursorAtIndex creates a new cursor over seq positioned at idx.
//
// Implemented by searching down the tree to the leaf sequence containing idx. Each
// sequence cursor includes a back pointer to its parent so that it can follow the path
// to the next leaf chunk when the cursor exhausts the entries in the current chunk.
func newCursorAtIndex(seq sequence, idx uint64) *sequenceCursor {
	var cur *sequenceCursor
	for {
		cur = newSequenceCursor(cur, seq, 0)
		idx = idx - advanceCursorToOffset(cur, idx)
		cs := cur.getChildSequence()
		if cs == nil {
			break
		}
		seq = cs
	}
	d.PanicIfTrue(cur == nil)
	return cur
}

// enableReadAhead turns on chunk read-ahead for a leaf sequence cursor.
// It is only intended to be called by sequenceIterator.
//
// Read-ahead should only be used in cases where the caller is iterating sequentially
// forward through all chunks starting at the current position.
func (cur *sequenceCursor) enableReadAhead() {
	_, meta := cur.seq.(metaSequence)
	d.PanicIfTrue(meta)
	if cur.parent != nil { // only operative if sequence is chunked
		cur.readAhead = newSequenceReadAhead(cur.parent, readAheadParallelism)
	}
}
