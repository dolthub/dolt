package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
)

type sequenceItem interface{}

type sequence interface {
	Value
	getItem(idx int) sequenceItem
	seqLen() int
}

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent *sequenceCursor
	seq    sequence
	idx    int
}

func newSequenceCursor(parent *sequenceCursor, seq sequence, idx int) *sequenceCursor {
	return &sequenceCursor{parent, seq, idx}
}

func (cur *sequenceCursor) length() int {
	return cur.seq.seqLen()
}

func (cur *sequenceCursor) getItem(idx int) sequenceItem {
	return cur.seq.getItem(idx)
}

func (cur *sequenceCursor) sync() {
	d.Chk.NotNil(cur.parent)
	cur.seq = cur.parent.getChildSequence()
}

func (cur *sequenceCursor) getChildSequence() sequence {
	return cur.seq.(metaSequence).getChildSequence(cur.idx)
}

// Returns the value the cursor refers to. Fails an assertion if the cursor doesn't point to a value.
func (cur *sequenceCursor) current() sequenceItem {
	item, ok := cur.maybeCurrent()
	d.Chk.True(ok)
	return item
}

// Returns the value the cursor refers to, if any. If the cursor doesn't point to a value, returns (nil, false).
func (cur *sequenceCursor) maybeCurrent() (sequenceItem, bool) {
	d.Chk.True(cur.idx >= -1 && cur.idx <= cur.length())
	if cur.idx == -1 || cur.idx == cur.length() {
		return nil, false
	}
	return cur.getItem(cur.idx), true
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
	d.Chk.Equal(0, cur.idx)
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

func (cur *sequenceCursor) clone() *sequenceCursor {
	var parent *sequenceCursor
	if cur.parent != nil {
		parent = cur.parent.clone()
	}
	return &sequenceCursor{parent, cur.seq, cur.idx}
}

type sequenceCursorSeekBinaryCompareFn func(item sequenceItem) bool

// seekBinary seeks the cursor to the first position in the sequence where |compare| returns true. This uses a binary search, so the cursor items must be sorted relative to |compare|. seekBinary will not seek past the end of the cursor.
func (cur *sequenceCursor) seekBinary(compare sequenceCursorSeekBinaryCompareFn) {
	d.Chk.NotNil(compare)

	if cur.parent != nil {
		cur.parent.seekBinary(compare)
		cur.sync()
	}

	cur.idx = sort.Search(cur.length(), func(i int) bool {
		return compare(cur.getItem(i))
	})

	if cur.idx == cur.length() {
		cur.idx = cur.length() - 1
	}
}

type sequenceCursorSeekLinearStepFn func(carryIn interface{}, item sequenceItem) (found bool, carryOut interface{})

// seekLinear seeks the cursor to the first position in the sequence where |step| returns true. This uses a linear search, so there is no ordering restriction. The carry value is initialized as |carry|, but will be replaced with the return value of successive calls to |step|, including when |step| is called on ancestor cursors. The return value is the carry value when seeking stopped. seekLinear will not seek past the end of the cursor.
func (cur *sequenceCursor) seekLinear(step sequenceCursorSeekLinearStepFn, carry interface{}) interface{} {
	d.Chk.NotNil(step)

	if cur.parent != nil {
		carry = cur.parent.seekLinear(step, carry)
		cur.sync()
	}

	cur.idx = 0
	for i := 0; i < cur.length()-1; i++ {
		found, carryOut := step(carry, cur.getItem(i))
		if found {
			break
		}
		carry = carryOut
		cur.idx++
	}

	return carry
}

// Returns a slice of the previous |n| items in |cur|, excluding the current item in |cur|. Does not modify |cur|.
func (cur *sequenceCursor) maxNPrevItems(n int) []sequenceItem {
	prev := []sequenceItem{}

	retreater := cur.clone()
	for i := 0; i < n && retreater.retreat(); i++ {
		prev = append(prev, retreater.current())
	}

	for i := 0; i < len(prev)/2; i++ {
		t := prev[i]
		prev[i] = prev[len(prev)-i-1]
		prev[len(prev)-i-1] = t
	}

	return prev
}
