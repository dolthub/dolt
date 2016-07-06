// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
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
		assert.Equal(float64(di.key.(types.Number)), float64(i))
		assert.Equal(testSize-i, dq.Len())
	}

	_, ok := dq.PopFront()
	assert.False(ok)
	assert.Equal(NewDiffQueue(), dq)
}
