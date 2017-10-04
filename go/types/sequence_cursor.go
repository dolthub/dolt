// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"
import "fmt"

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent    *sequenceCursor
	seq       sequence
	idx       int
	readAhead bool
	childSeqs []sequence
	seqLen    int
}

// Advances |sc| forward and streams back a clone of |sc| for each distinct sequence its *parent*
// visits. The caller should read the leaf cursor off of |curChan| and advance() until invalid.
// |readAheadLeafCursor| will |preloadChildren()| on each distinct parent cursor prior to sending it
// to |curChan|. The effect of this is that the client will be iterating over a sequence of
// leaf + 1 prolly tree sequences, each of which will have preloaded its children.
//
//     /---\       /---\
//  _______________________    <- each Cx's grandparent will be nil so that it only advances within a single sequence
//   / \   / \   / \   / \     <- first meta-level
//  /\ /\ /\ /\ /\ /\ /\ /\    <- leaf level
//  ^     ^     ^     ^
//  |     |     |     |
//  c1    c2    c3    c3  <- |curChan|
//
func readAheadLeafCursors(sc *sequenceCursor, curChan chan chan *sequenceCursor, stopChan chan struct{}) {
	d.Chk.True(sc.seq.isLeaf())

	parentCursor := sc.parent
	if parentCursor == nil {
		ch := make(chan *sequenceCursor, 1)
		curChan <- ch
		ch <- sc // No meta level on which to read ahead
		return
	}

	d.Chk.True(parentCursor.readAhead)
	leafIdx := sc.idx // Ensure the first cursor delivered is at the correct start position

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		if !parentCursor.valid() {
			break // end of meta level has been reached
		}

		ch := make(chan *sequenceCursor)
		curChan <- ch

		pc := newSequenceCursor(nil, parentCursor.seq, parentCursor.idx, true)

		go func(ch chan *sequenceCursor, pc *sequenceCursor, leafIdx int) {
			ch <- newSequenceCursor(pc, pc.getChildSequence(), leafIdx, true)
		}(ch, pc, leafIdx)

		for parentCursor.advance() && parentCursor.idx > 0 {
		}

		leafIdx = 0
	}
}

// newSequenceCursor creates a cursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newSequenceCursor(parent *sequenceCursor, seq sequence, idx int, readAhead bool) *sequenceCursor {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}

	readAhead = readAhead && !seq.isLeaf() && seq.valueReadWriter() != nil
	return &sequenceCursor{parent, seq, idx, readAhead, nil, seqLen}
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
	cur.childSeqs = nil
	cur.seq = cur.parent.getChildSequence()
	cur.seqLen = cur.seq.seqLen()
}

func (cur *sequenceCursor) preloadChildren() {
	if cur.childSeqs != nil {
		return
	}

	cur.childSeqs = make([]sequence, cur.seq.seqLen())
	ms := cur.seq.(metaSequence)
	copy(cur.childSeqs[cur.idx:], ms.getChildren(uint64(cur.idx), uint64(cur.seq.seqLen())))
}

// getChildSequence retrieves the child at the current cursor position.
func (cur *sequenceCursor) getChildSequence() sequence {
	if cur.readAhead {
		cur.preloadChildren()
		if child := cur.childSeqs[cur.idx]; child != nil {
			return child
		}
	}

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
	cl := newSequenceCursor(parent, cur.seq, cur.idx, cur.readAhead)
	cl.childSeqs = cur.childSeqs
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
	if cur.parent == nil || !cur.parent.readAhead {
		for cur.valid() && !cb(cur.getItem(cur.idx)) {
			cur.advance()
		}
		return
	}

	curChan := make(chan chan *sequenceCursor, 16) // read ahead ~ 10MB of leaf sequence
	stopChan := make(chan struct{}, 1)

	go func() {
		readAheadLeafCursors(cur, curChan, stopChan)
		close(curChan)
	}()

	for ch := range curChan {
		leafCursor := <-ch
		for leafCursor.valid() {
			if cb(leafCursor.getItem(leafCursor.idx)) {
				stopChan <- struct{}{}
				for range curChan {
				} // ensure async loading goroutine exits before we do
				return
			}

			leafCursor.advance()
		}
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
		cur = newSequenceCursor(cur, seq, 0, false)
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
