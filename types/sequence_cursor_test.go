package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func newTestSequenceCursor(items [][]int) *sequenceCursor {
	parent := &sequenceCursor{nil, items, 0, len(items), func(item sequenceItem, idx int) sequenceItem {
		return item.([][]int)[idx] // item should be == items
	}, func(item sequenceItem) (sequenceItem, int) {
		panic("not reachable")
	}}

	return &sequenceCursor{parent, items[0], 0, len(items[0]), func(item sequenceItem, idx int) sequenceItem {
		return item.([]int)[idx]
	}, func(item sequenceItem) (sequenceItem, int) {
		return item, len(item.([]int))
	}}
}

func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([][]int{[]int{100, 101}, []int{102}})
	}
	expect := func(expectIdx, expectParentIdx int, expectOk bool, expectVal sequenceItem) {
		assert.Equal(expectIdx, cur.indexInChunk())
		assert.Equal(expectParentIdx, cur.parent.indexInChunk())
		val, ok := cur.maybeCurrent()
		assert.Equal(expectOk, ok)
		assert.Equal(expectVal, val)
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

func TestCursorGetMaxNPrevItemsWithEmptySequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{}})
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
}

func TestCursorGetMaxNPrevItemsWithSingleItemSequence(t *testing.T) {
	assert := assert.New(t)
	cur := newTestSequenceCursor([][]int{[]int{100}, []int{101}, []int{102}})

	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{101}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{102}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(3))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(4))
	assert.Equal(1, cur.idx)
}

func TestCursorGetMaxNPrevItemsWithMultiItemSequence(t *testing.T) {
	assert := assert.New(t)

	cur := newTestSequenceCursor([][]int{
		[]int{100, 101, 102, 103},
		[]int{104, 105, 106, 107},
	})

	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100}, cur.maxNPrevItems(3))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{101}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101}, cur.maxNPrevItems(3))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{102}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{101, 102}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{100, 101, 102}, cur.maxNPrevItems(3))
	assert.Equal(3, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{103}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{102, 103}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{101, 102, 103}, cur.maxNPrevItems(3))
	assert.Equal(0, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{104}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{103, 104}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{102, 103, 104}, cur.maxNPrevItems(3))
	assert.Equal(1, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{105}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{104, 105}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{103, 104, 105}, cur.maxNPrevItems(3))
	assert.Equal(2, cur.idx)

	assert.True(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{106}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{105, 106}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{104, 105, 106}, cur.maxNPrevItems(3))
	assert.Equal(3, cur.idx)

	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cur.maxNPrevItems(7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106}, cur.maxNPrevItems(8))

	assert.False(cur.advance())
	assert.Equal([]sequenceItem{}, cur.maxNPrevItems(0))
	assert.Equal([]sequenceItem{107}, cur.maxNPrevItems(1))
	assert.Equal([]sequenceItem{106, 107}, cur.maxNPrevItems(2))
	assert.Equal([]sequenceItem{105, 106, 107}, cur.maxNPrevItems(3))
	assert.Equal(4, cur.idx)

	assert.Equal([]sequenceItem{101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(7))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(8))
	assert.Equal([]sequenceItem{100, 101, 102, 103, 104, 105, 106, 107}, cur.maxNPrevItems(9))
}

func TestCursorSeek(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([][]int{
			[]int{100, 101, 102, 103},
			[]int{104, 105, 106, 107},
		})
	}

	assertSeeksTo := func(expected sequenceItem, seekTo int) {
		// The value being carried around here is the level of the tree being seeked in. The seek is initialized with 0, so carry value passed to the comparison function on the first level should be 0. Subsequent steps increment this number, so 1 should be passed into the comparison function for the second level. When the seek exits, the final step should increment it again, so the result should be 2.
		result := cur.seek(func(carry interface{}, val sequenceItem) bool {
			switch val := val.(type) {
			case []int:
				assert.Equal(0, carry)
				return val[len(val)-1] >= seekTo
			case int:
				assert.Equal(1, carry)
				return val >= seekTo
			default:
				panic("illegal")
			}
		}, func(carry interface{}, prev, current sequenceItem) interface{} {
			switch current.(type) {
			case []int:
				assert.Equal(0, carry)
			case int:
				assert.Equal(1, carry)
			}
			return carry.(int) + 1
		}, 0)
		assert.Equal(2, result)
		assert.Equal(expected, cur.current())
	}

	// Test seeking immediately to values on cursor construction.
	reset()
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		reset()
		assertSeeksTo(sequenceItem(i), i)
	}
	reset()
	assertSeeksTo(sequenceItem(107), 108)

	// Test reusing an existing cursor to seek all over the place.
	reset()
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		assertSeeksTo(sequenceItem(i), i)
	}
	assertSeeksTo(sequenceItem(107), 108)
	assertSeeksTo(sequenceItem(100), 99)
	for i := 100; i <= 107; i++ {
		assertSeeksTo(sequenceItem(i), i)
	}
	assertSeeksTo(sequenceItem(107), 108)
}
