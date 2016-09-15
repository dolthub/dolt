// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestRefByHeight(t *testing.T) {
	unique := 0
	newRefWithHeight := func(height uint64) Ref {
		r := NewRef(Number(unique))
		unique++
		r.height = height
		return r
	}

	assert := assert.New(t)

	h := RefByHeight{}

	r1 := newRefWithHeight(1)
	r2 := newRefWithHeight(2)
	r3 := newRefWithHeight(3)
	r4 := newRefWithHeight(2)

	h.PushBack(r1)
	assert.Equal(r1, h.PeekEnd())
	assert.Equal(1, len(h))

	h.PushBack(r3)
	sort.Sort(&h)
	assert.Equal(r3, h.PeekEnd())
	assert.Equal(2, len(h))

	h.PushBack(r2)
	sort.Sort(&h)
	assert.Equal(r3, h.PeekEnd())
	assert.Equal(3, len(h))

	h.PushBack(r4)
	sort.Sort(&h)
	assert.Equal(r3, h.PeekEnd())
	assert.Equal(4, len(h))

	expectedSecond, expectedThird := func() (Ref, Ref) {
		if r2.TargetHash().Less(r4.TargetHash()) {
			return r2, r4
		}
		return r4, r2
	}()

	assert.Equal(r3, h.PopBack())
	assert.Equal(expectedSecond, h.PeekEnd())
	assert.Equal(3, len(h))

	assert.Equal(expectedSecond, h.PopBack())
	assert.Equal(expectedThird, h.PeekEnd())
	assert.Equal(2, len(h))

	assert.Equal(expectedThird, h.PopBack())
	assert.Equal(r1, h.PeekEnd())
	assert.Equal(1, len(h))

	assert.Equal(r1, h.PopBack())
	assert.Equal(0, len(h))
}

func TestDropIndices(t *testing.T) {
	h := &RefByHeight{}
	for i := 0; i < 10; i++ {
		h.PushBack(NewRef(Number(i)))
	}
	sort.Sort(h)

	toDrop := []int{2, 4, 7}
	expected := RefSlice{h.PeekAt(2), h.PeekAt(4), h.PeekAt(7)}
	h.DropIndices(toDrop)
	assert.Len(t, *h, 7)
	for i, dropped := range expected {
		assert.NotContains(t, *h, dropped, "Should not contain %d", toDrop[i])
	}
}

func TestPopRefsOfHeight(t *testing.T) {
	h := &RefByHeight{}
	for i, n := range []int{6, 3, 6, 6, 2} {
		r := NewRef(Number(i))
		r.height = uint64(n)
		h.PushBack(r)
	}
	sort.Sort(h)

	expected := RefSlice{h.PeekAt(4), h.PeekAt(3), h.PeekAt(2)}
	refs := h.PopRefsOfHeight(h.MaxHeight())
	assert.Len(t, *h, 2)
	assert.Len(t, refs, 3)
	for _, popped := range expected {
		assert.NotContains(t, *h, popped, "Should not contain ref of height 6")
	}
}
