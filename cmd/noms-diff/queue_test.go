package main

import (
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/assert"
)

func TestQueue(t *testing.T) {
	assert := assert.New(t)
	const testSize = 4
	dq := NewDiffQueue()

	for i := 1; i <= testSize; i++ {
		dq.PushBack(diffInfo{key: types.Number(i)})
		assert.Equal(i, dq.Len())
	}

	for i := 1; i <= testSize; i++ {
		di, ok := dq.PopFront()
		assert.True(ok)
		assert.Equal(di.key.(types.Number).ToPrimitive().(float64), float64(i))
		assert.Equal(testSize-i, dq.Len())
	}

	_, ok := dq.PopFront()
	assert.False(ok)
	assert.Equal(NewDiffQueue(), dq)
}
