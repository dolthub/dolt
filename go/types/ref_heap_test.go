// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"container/heap"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestRefHeap(t *testing.T) {
	unique := 0
	newRefWithHeight := func(height uint64) Ref {
		r := NewRef(Number(unique))
		unique++
		r.height = height
		return r
	}

	assert := assert.New(t)

	h := RefHeap{}
	heap.Init(&h)

	r1 := newRefWithHeight(1)
	r2 := newRefWithHeight(2)
	r3 := newRefWithHeight(3)
	r4 := newRefWithHeight(2)

	heap.Push(&h, r1)
	assert.Equal(r1, h[0])
	assert.Equal(1, len(h))

	heap.Push(&h, r3)
	assert.Equal(r3, h[0])
	assert.Equal(2, len(h))

	heap.Push(&h, r2)
	assert.Equal(r3, h[0])
	assert.Equal(3, len(h))

	heap.Push(&h, r4)
	assert.Equal(r3, h[0])
	assert.Equal(4, len(h))

	expectedSecond, expectedThird := func() (Ref, Ref) {
		if r2.TargetHash().Less(r4.TargetHash()) {
			return r2, r4
		}
		return r4, r2
	}()

	assert.Equal(r3, heap.Pop(&h).(Ref))
	assert.Equal(expectedSecond, h[0])
	assert.Equal(3, len(h))

	assert.Equal(expectedSecond, heap.Pop(&h).(Ref))
	assert.Equal(expectedThird, h[0])
	assert.Equal(2, len(h))

	assert.Equal(expectedThird, heap.Pop(&h).(Ref))
	assert.Equal(r1, h[0])
	assert.Equal(1, len(h))

	assert.Equal(r1, heap.Pop(&h).(Ref))
	assert.Equal(0, len(h))
}
