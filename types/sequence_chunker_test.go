package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestSequenceChunkerMod(t *testing.T) {
	assert := assert.New(t)

	sumChunker := func(items []sequenceItem) (sequenceItem, interface{}) {
		sum := 0
		for _, item := range items {
			sum += item.(int)
		}
		return sum, items
	}

	modBounder := func(mod int) isBoundaryFn {
		return func(item sequenceItem) bool {
			return item.(int)%mod == 0
		}
	}

	newTestSequenceChunker := func(from, to int) *sequenceChunker {
		seq := newSequenceChunker(sumChunker, sumChunker, modBounder(3), modBounder(5))
		for i := from; i <= to; i++ {
			seq.Append(i)
		}
		return seq
	}

	intsFromSequenceItems := func(items interface{}) []int {
		intSlice := []int{}
		for _, item := range items.([]sequenceItem) {
			intSlice = append(intSlice, item.(int))
		}
		return intSlice
	}

	// [1] is not a chunk boundary, so it won't chunk.
	seq := newTestSequenceChunker(1, 1)
	sum, items := seq.Done()
	assert.Equal(1, sum)
	assert.Equal([]int{1}, intsFromSequenceItems(items))

	// [3] is a chunk boundary, but only a single chunk, so treat it as though it didn't chunk.
	seq = newTestSequenceChunker(3, 3)
	sum, items = seq.Done()
	assert.Equal(3, sum)
	assert.Equal([]int{3}, intsFromSequenceItems(items))

	// None of [1, 2] is a chunk boundary, so it won't chunk.
	seq = newTestSequenceChunker(1, 2)
	sum, items = seq.Done()
	assert.Equal(3, sum)
	assert.Equal([]int{1, 2}, intsFromSequenceItems(items))

	// [3, 4] has a chunk boundary on 3, so it should chunk as [3] [4].
	seq = newTestSequenceChunker(3, 4)
	sum, items = seq.Done()
	assert.Equal(7, sum)
	assert.Equal([]int{3, 4}, intsFromSequenceItems(items))

	// [1, 2, 3] ends in a chunk boundary 3, but only a single chunk, so treat is as though it didn't chunk.
	seq = newTestSequenceChunker(1, 3)
	sum, items = seq.Done()
	assert.Equal(6, sum)
	assert.Equal([]int{1, 2, 3}, intsFromSequenceItems(items))

	// [1, 2, 3, 4] has a chunk boundary on 3, so should chunk as [1, 2, 3] [4].
	seq = newTestSequenceChunker(1, 4)
	sum, items = seq.Done()
	assert.Equal(10, sum)
	assert.Equal([]int{6, 4}, intsFromSequenceItems(items))

	// [1, 2, 3, 4, 5, 6] has a chunk boundary on 3 and 6, so should chunk as [1, 2, 3] [4, 5, 6]. Note also that the level above that ends in a chunk boundary 15 because 4+5+16=15 and 15%5 = 0, but like earlier, this shouldn't chunk because it's only a single chunk.
	seq = newTestSequenceChunker(1, 6)
	sum, items = seq.Done()
	assert.Equal(21, sum)
	assert.Equal([]int{6, 15}, intsFromSequenceItems(items))

	// [1, 2, 3, 4, 5, 6, 7] will chunk as [1, 2, 3] [4, 5, 6] [7] producing meta chunks of [6, 15, 7] which chunks on 15, so we'll get 2 chunks [6, 15] [7].
	seq = newTestSequenceChunker(1, 7)
	sum, items = seq.Done()
	assert.Equal(28, sum)
	assert.Equal([]int{21, 7}, intsFromSequenceItems(items))
}
