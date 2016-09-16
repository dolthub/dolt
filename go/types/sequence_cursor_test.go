// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

type testSequence struct {
	items []interface{}
}

// sequence interface
func (ts testSequence) getItem(idx int) sequenceItem {
	return ts.items[idx]
}

func (ts testSequence) seqLen() int {
	return len(ts.items)
}

func (ts testSequence) numLeaves() uint64 {
	return uint64(len(ts.items))
}

func (ts testSequence) getCompareFn(other sequence) compareFn {
	obl := other.(testSequence)
	return func(idx, otherIdx int) bool {
		return ts.items[idx] == obl.items[otherIdx]
	}
}

func (ts testSequence) valueReader() ValueReader {
	panic("not reached")
}

func (ts testSequence) getChildSequence(idx int) sequence {
	child := ts.items[idx]
	return testSequence{child.([]interface{})}
}

// Value interface
func (ts testSequence) Equals(other Value) bool {
	panic("not reached")
}

func (ts testSequence) Less(other Value) bool {
	panic("not reached")
}

func (ts testSequence) Hash() hash.Hash {
	panic("not reached")
}

func (ts testSequence) ChildValues() []Value {
	panic("not reached")
}

func (ts testSequence) Chunks() []Ref {
	panic("not reached")
}

func (ts testSequence) Type() *Type {
	panic("not reached")
}

func newTestSequenceCursor(items []interface{}) *sequenceCursor {
	parent := newSequenceCursor(nil, testSequence{items}, 0)
	items = items[0].([]interface{})
	return newSequenceCursor(parent, testSequence{items}, 0)
}

// TODO: Convert all tests to use newTestSequenceCursor3.
func newTestSequenceCursor3(items []interface{}) *sequenceCursor {
	top := newSequenceCursor(nil, testSequence{items}, 0)
	items = items[0].([]interface{})
	middle := newSequenceCursor(top, testSequence{items}, 0)
	items = items[0].([]interface{})
	return newSequenceCursor(middle, testSequence{items}, 0)
}

func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([]interface{}{[]interface{}{100, 101}, []interface{}{102}})
	}
	expect := func(expectIdx, expectParentIdx int, expectOk bool, expectVal sequenceItem) {
		assert.Equal(expectIdx, cur.indexInChunk())
		assert.Equal(expectParentIdx, cur.parent.indexInChunk())
		assert.Equal(expectOk, cur.valid())
		if cur.valid() {
			assert.Equal(expectVal, cur.current())
		}
	}

	// Test retreating past the start.
	reset()
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)

	// Test retreating past the start, then advanding past the end.
	reset()
	assert.False(cur.retreat())
	assert.True(cur.advance())
	expect(0, 0, true, sequenceItem(100))
	assert.True(cur.advance())
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.advance())
	expect(0, 1, true, sequenceItem(102))
	assert.False(cur.advance())
	expect(1, 1, false, nil)
	assert.False(cur.advance())
	expect(1, 1, false, nil)

	// Test advancing past the end.
	reset()
	assert.True(cur.advance())
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.retreat())
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)

	// Test advancing past the end, then retreating past the start.
	reset()
	assert.True(cur.advance())
	assert.True(cur.advance())
	expect(0, 1, true, sequenceItem(102))
	assert.False(cur.advance())
	expect(1, 1, false, nil)
	assert.False(cur.advance())
	expect(1, 1, false, nil)
	assert.True(cur.retreat())
	expect(0, 1, true, sequenceItem(102))
	assert.True(cur.retreat())
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.retreat())
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)
	assert.False(cur.retreat())
	expect(-1, 0, false, nil)
}
