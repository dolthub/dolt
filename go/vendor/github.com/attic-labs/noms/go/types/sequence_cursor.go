// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"
import "fmt"

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent *sequenceCursor
	seq    sequence
	idx    int
	seqLen int
}

// newSequenceCursor creates a cursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newSequenceCursor(parent *sequenceCursor, seq sequence, idx int) *sequenceCursor {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}

	return &sequenceCursor{parent, seq, idx, seqLen}
}

func (cur *sequenceCursor) length() int {
	return cur.seqLen
}

func (cur *sequenceCursor) getItem(idx int) sequenceItem {
	return cur.seq.getItem(idx)
}

// sync loads the sequence that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *sequenceCursor) sync() {
	d.PanicIfFalse(cur.parent != nil)
	cur.seq = cur.parent.getChildSequence()
	cur.seqLen = cur.seq.seqLen()
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

func (cur *sequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *sequenceCursor) atLastItem() bool {
	return cur.idx == cur.length()-1
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
	cl := newSequenceCursor(parent, cur.seq, cur.idx)
	return cl
}

type cursorIterCallback func(item interface{}) bool

func (cur *sequenceCursor) String() string {
	if cur.parent == nil {
		return fmt.Sprintf("%s (%d): %d", newMap(cur.seq.(orderedSequence)).Hash().String(), cur.seq.seqLen(), cur.idx)
	}

	return fmt.Sprintf("%s (%d): %d -- %s", newMap(cur.seq.(orderedSequence)).Hash().String(), cur.seq.seqLen(), cur.idx, cur.parent.String())
}

func (cur *sequenceCursor) compare(other *sequenceCursor) int {
	if cur.parent != nil {
		d.PanicIfFalse(other.parent != nil)
		p := cur.parent.compare(other.parent)
		if p != 0 {
			return p
		}

	}

	// TODO: It'd be nice here to assert that the two sequences are the same
	// but there isn't a good way to that at this point because the containing
	// collection of the sequence isn't available.
	d.PanicIfFalse(cur.seq.seqLen() == other.seq.seqLen())
	return cur.idx - other.idx
}

// iter iterates forward from the current position
func (cur *sequenceCursor) iter(cb cursorIterCallback) {
	for cur.valid() && !cb(cur.getItem(cur.idx)) {
		cur.advance()
	}
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
