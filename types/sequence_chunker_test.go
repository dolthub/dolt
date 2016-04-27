package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type modBoundaryChecker struct {
	mod int
}

func (b modBoundaryChecker) Write(item sequenceItem) bool {
	return item.(int)%b.mod == 0
}

func (b modBoundaryChecker) WindowSize() int {
	return 1
}

func listFromInts(ints []int) List {
	vals := make([]Value, len(ints))
	for i, v := range ints {
		vals[i] = Number(v)
	}

	return NewList(vals...)
}

func TestSequenceChunkerMod(t *testing.T) {
	assert := assert.New(t)

	sumChunker := func(items []sequenceItem) (sequenceItem, Value) {
		sum := 0
		ints := make([]int, len(items))
		for i, item := range items {
			v := item.(int)
			ints[i] = v
			sum += v
		}
		return sum, listFromInts(ints)
	}

	testChunking := func(expect []int, from, to int) {
		seq := newEmptySequenceChunker(sumChunker, sumChunker, modBoundaryChecker{3}, func() boundaryChecker { return modBoundaryChecker{5} })
		for i := from; i <= to; i++ {
			seq.Append(i)
		}

		assert.True(listFromInts(expect).Equals(seq.Done()))
	}

	// [1] is not a chunk boundary, so it won't chunk.
	testChunking([]int{1}, 1, 1)

	// [3] is a chunk boundary, but only a single chunk, so treat it as though it didn't chunk.
	testChunking([]int{3}, 3, 3)

	// None of [1, 2] is a chunk boundary, so it won't chunk.
	testChunking([]int{1, 2}, 1, 2)

	// [3, 4] has a chunk boundary on 3, so it should chunk as [3] [4].
	testChunking([]int{3, 4}, 3, 4)

	// [1, 2, 3] ends in a chunk boundary 3, but only a single chunk, so treat is as though it didn't chunk.
	testChunking([]int{1, 2, 3}, 1, 3)

	// [1, 2, 3, 4] has a chunk boundary on 3, so should chunk as [1, 2, 3] [4].
	testChunking([]int{6, 4}, 1, 4)

	// [1, 2, 3, 4, 5, 6] has a chunk boundary on 3 and 6, so should chunk as [1, 2, 3] [4, 5, 6]. Note also that the level above that ends in a chunk boundary 15 because 4+5+16=15 and 15%5 = 0, but like earlier, this shouldn't chunk because it's only a single chunk.
	testChunking([]int{6, 15}, 1, 6)

	// [1, 2, 3, 4, 5, 6, 7] will chunk as [1, 2, 3] [4, 5, 6] [7] producing meta chunks of [6, 15, 7] which chunks on 15, so we'll get 2 chunks [6, 15] [7].
	testChunking([]int{21, 7}, 1, 7)
}
