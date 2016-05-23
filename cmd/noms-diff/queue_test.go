package main

import (
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/assert"
)

func TestQueue(t *testing.T) {
	assert := assert.New(t)
	const testSize = 4
	dq := diffQueue{}

	for i := 1; i <= testSize; i++ {
		dq.Push(diffInfo{key: types.Number(i)})
		assert.Equal(i, dq.len)
	}

	for i := 1; i <= testSize; i++ {
		di, ok := dq.Pop()
		assert.True(ok)
		assert.Equal(di.key.(types.Number).ToPrimitive().(float64), float64(i))
		assert.Equal(testSize-i, dq.len)
	}

	_, ok := dq.Pop()
	assert.False(ok)
	assert.Equal(diffQueue{}, dq)
}
