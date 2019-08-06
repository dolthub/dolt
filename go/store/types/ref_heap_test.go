// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefByHeight(t *testing.T) {
	unique := 0
	newRefWithHeight := func(height uint64) (Ref, error) {
		v := Float(unique)
		unique++
		h, err := v.Hash(Format_7_18)

		if err != nil {
			return Ref{}, err
		}

		return constructRef(Format_7_18, h, FloaTType, height)
	}

	assert := assert.New(t)

	h := RefByHeight{}

	r1, err := newRefWithHeight(1)
	assert.NoError(err)
	r2, err := newRefWithHeight(2)
	assert.NoError(err)
	r3, err := newRefWithHeight(3)
	assert.NoError(err)
	r4, err := newRefWithHeight(2)
	assert.NoError(err)

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
		ref, err := NewRef(Float(i), Format_7_18)
		assert.NoError(t, err)
		h.PushBack(ref)
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
		hsh, err := Float(i).Hash(Format_7_18)
		assert.NoError(t, err)
		r, err := constructRef(Format_7_18, hsh, FloaTType, uint64(n))
		assert.NoError(t, err)
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
