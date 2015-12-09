package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
)

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent      *sequenceCursor
	item        sequenceItem
	idx, length int
	getItem     getItemFn
	readChunk   readChunkFn
}

// getItemFn takes a parent in the sequence and an index into that parent, and returns the child item, equivalent to `child = parent[idx]`. The parent and the child aren't necessarily the same type.
type getItemFn func(parent sequenceItem, idx int) (child sequenceItem)

// readChunkFn takes an item in the sequence which references another sequence of items, and returns that sequence along with its length.
type readChunkFn func(reference sequenceItem) (sequence sequenceItem, length int)

// Returns the value the cursor refers to. Fails an assertion if the cursor doesn't point to a value.
func (cur *sequenceCursor) current() sequenceItem {
	item, ok := cur.maybeCurrent()
	d.Chk.True(ok)
	return item
}

// Returns the value the cursor refers to, if any. If the cursor doesn't point to a value, returns (nil, false).
func (cur *sequenceCursor) maybeCurrent() (sequenceItem, bool) {
	d.Chk.True(cur.idx >= -1 && cur.idx <= cur.length)
	if cur.idx == -1 || cur.idx == cur.length {
		return nil, false
	} else {
		return cur.getItem(cur.item, cur.idx), true
	}
}

func (cur *sequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *sequenceCursor) advance() bool {
	return cur.advanceMaybeAllowPastEnd(true)
}

func (cur *sequenceCursor) advanceMaybeAllowPastEnd(allowPastEnd bool) bool {
	if cur.idx < cur.length-1 {
		cur.idx++
		return true
	}
	if cur.idx == cur.length {
		return false
	}
	if cur.parent != nil && cur.parent.advanceMaybeAllowPastEnd(false) {
		cur.item, cur.length = cur.readChunk(cur.parent.current())
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
		cur.item, cur.length = cur.readChunk(cur.parent.current())
		cur.idx = cur.length - 1
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
	return &sequenceCursor{parent, cur.item, cur.idx, cur.length, cur.getItem, cur.readChunk}
}

type sequenceCursorSeekCompareFn func(carry interface{}, item sequenceItem) bool

type sequenceCursorSeekStepFn func(carry interface{}, prev, current sequenceItem) interface{}

// Seeks the cursor to the first position in the sequence where |compare| returns true. During seeking, the caller can build up an arbitrary carry value, passed to |compare| and |step|. The carry value is initialized as |carry|, but will be replaced with the return value of |step|.
func (cur *sequenceCursor) seek(compare sequenceCursorSeekCompareFn, step sequenceCursorSeekStepFn, carry interface{}) interface{} {
	d.Chk.NotNil(compare)

	if cur.parent != nil {
		carry = cur.parent.seek(compare, step, carry)
		cur.item, cur.length = cur.readChunk(cur.parent.current())
	}

	cur.idx = sort.Search(cur.length, func(i int) bool {
		return compare(carry, cur.getItem(cur.item, i))
	})

	if cur.idx == cur.length {
		cur.idx = cur.length - 1
	}

	var prev sequenceItem
	if cur.idx > 0 {
		prev = cur.getItem(cur.item, cur.idx-1)
	}

	if step == nil {
		return nil
	}

	return step(carry, prev, cur.getItem(cur.item, cur.idx))
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
