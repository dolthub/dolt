package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

// TODO: Convert all tests to use newTestSequenceCursor3.
func newTestSequenceCursor3(items [][][]int) *sequenceCursor {
	top := &sequenceCursor{nil, items, 0, len(items), func(item sequenceItem, idx int) sequenceItem {
		return item.([][][]int)[idx] // item should be == items
	}, func(item sequenceItem) (sequenceItem, int) {
		panic("not reachable")
	}}

	middle := &sequenceCursor{top, items[0], 0, len(items[0]), func(item sequenceItem, idx int) sequenceItem {
		return item.([][]int)[idx]
	}, func(item sequenceItem) (sequenceItem, int) {
		return item, len(item.([][]int))
	}}

	return &sequenceCursor{middle, items[0][0], 0, len(items[0][0]), func(item sequenceItem, idx int) sequenceItem {
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

func TestCursorSeekBinary(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([][]int{
			[]int{100, 101, 102, 103},
			[]int{104, 105, 106, 107},
		})
	}

	assertSeeksTo := func(expected sequenceItem, seekTo int) {
		cur.seekBinary(func(val sequenceItem) bool {
			switch val := val.(type) {
			case []int:
				return val[len(val)-1] >= seekTo
			case int:
				return val >= seekTo
			default:
				panic("illegal")
			}
		})
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

func TestCursorSeekLinear(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor

	assertSeeksTo := func(reset bool, expectedPos sequenceItem, expectedSumUpto, seekTo int) {
		if reset {
			cur = newTestSequenceCursor3(
				[][][]int{
					[][]int{
						[]int{100, 101, 102, 103},
						[]int{104, 105, 106, 107},
					},
					[][]int{
						[]int{108, 109, 110, 111},
						[]int{112, 113, 114, 115},
					},
				},
			)
		}
		sumUpto := cur.seekLinear(func(carry interface{}, item sequenceItem) (bool, interface{}) {
			switch item := item.(type) {
			case [][]int:
				last := item[len(item)-1]
				return seekTo <= last[len(last)-1], carry
			case []int:
				return seekTo <= item[len(item)-1], carry
			case int:
				return seekTo <= item, item + carry.(int)
			}
			panic("illegal")
		}, 0)
		pos, _ := cur.maybeCurrent()
		assert.Equal(expectedPos, pos)
		assert.Equal(expectedSumUpto, sumUpto)
	}

	// Test seeking immediately to values on cursor construction.
	assertSeeksTo(true, sequenceItem(100), 0, 99)

	assertSeeksTo(true, sequenceItem(100), 0, 100)
	assertSeeksTo(true, sequenceItem(101), 100, 101)
	assertSeeksTo(true, sequenceItem(102), 201, 102)
	assertSeeksTo(true, sequenceItem(103), 303, 103)

	assertSeeksTo(true, sequenceItem(104), 0, 104)
	assertSeeksTo(true, sequenceItem(105), 104, 105)
	assertSeeksTo(true, sequenceItem(106), 209, 106)
	assertSeeksTo(true, sequenceItem(107), 315, 107)

	assertSeeksTo(true, sequenceItem(108), 0, 108)
	assertSeeksTo(true, sequenceItem(109), 108, 109)
	assertSeeksTo(true, sequenceItem(110), 217, 110)
	assertSeeksTo(true, sequenceItem(111), 327, 111)

	assertSeeksTo(true, sequenceItem(112), 0, 112)
	assertSeeksTo(true, sequenceItem(113), 112, 113)
	assertSeeksTo(true, sequenceItem(114), 225, 114)
	assertSeeksTo(true, sequenceItem(115), 339, 115)

	assertSeeksTo(true, sequenceItem(115), 339, 116)

	// Test reusing an existing cursor to seek all over the place.
	assertSeeksTo(false, sequenceItem(100), 0, 99)

	assertSeeksTo(false, sequenceItem(100), 0, 100)
	assertSeeksTo(false, sequenceItem(101), 100, 101)
	assertSeeksTo(false, sequenceItem(102), 201, 102)
	assertSeeksTo(false, sequenceItem(103), 303, 103)

	assertSeeksTo(false, sequenceItem(104), 0, 104)
	assertSeeksTo(false, sequenceItem(105), 104, 105)
	assertSeeksTo(false, sequenceItem(106), 209, 106)
	assertSeeksTo(false, sequenceItem(107), 315, 107)

	assertSeeksTo(false, sequenceItem(108), 0, 108)
	assertSeeksTo(false, sequenceItem(109), 108, 109)
	assertSeeksTo(false, sequenceItem(110), 217, 110)
	assertSeeksTo(false, sequenceItem(111), 327, 111)

	assertSeeksTo(false, sequenceItem(112), 0, 112)
	assertSeeksTo(false, sequenceItem(113), 112, 113)
	assertSeeksTo(false, sequenceItem(114), 225, 114)
	assertSeeksTo(false, sequenceItem(115), 339, 115)

	assertSeeksTo(false, sequenceItem(115), 339, 116)
}
