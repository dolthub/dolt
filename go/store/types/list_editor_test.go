// Copyright 2019 Dolthub, Inc.
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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listOfInts(vrw ValueReadWriter, vals ...int) (List, error) {
	vs := ValueSlice{}
	for _, v := range vals {
		vs = append(vs, Float(v))
	}
	return NewList(context.Background(), vrw, vs...)
}

func testEditor(vrw ValueReadWriter, vals ...int) *ListEditor {
	return NewListEditor(mustList(listOfInts(vrw, vals...)))
}

func edit(le *ListEditor, idx, remove int, insert ...int) {
	vals := []Valuable{}
	for _, v := range insert {
		vals = append(vals, Float(v))
	}
	le.Splice(uint64(idx), uint64(remove), vals...)
}

func assertState(t *testing.T, vrw ValueReadWriter, le *ListEditor, expectItems []int, expectEditCount int) {
	assert.Equal(t, uint64(len(expectItems)), le.Len())

	for i, v := range expectItems {
		item, err := le.Get(context.Background(), uint64(i))
		require.NoError(t, err)
		assert.Equal(t, Float(v), item)
	}

	actualEditCount := 0
	for edit := le.edits; edit != nil; edit = edit.next {
		actualEditCount++
	}

	assert.Equal(t, expectEditCount, actualEditCount)

	l, err := listOfInts(vrw, expectItems...)
	require.NoError(t, err)
	l2, err := le.List(context.Background())
	require.NoError(t, err)
	assert.True(t, l.Equals(l2))
}

func TestListEditorBasic(t *testing.T) {
	vrw := newTestValueStore()

	t.Run("remove  a few", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 2)
		assertState(t, vrw, le, []int{0, 1, 4, 5}, 1)
	})

	t.Run("insert  a few", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 0, 9, 8, 7)
		assertState(t, vrw, le, []int{0, 1, 9, 8, 7, 2, 3, 4, 5}, 1)
	})

	t.Run("remove 2, insert 3", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 2, 9, 8, 7)
		assertState(t, vrw, le, []int{0, 1, 9, 8, 7, 4, 5}, 1)
	})

	t.Run("insert 2 twice", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 0, 9, 10)
		assertState(t, vrw, le, []int{0, 1, 9, 10, 2, 3, 4, 5}, 1)
		edit(le, 7, 0, 8, 9)
		assertState(t, vrw, le, []int{0, 1, 9, 10, 2, 3, 4, 8, 9, 5}, 2)
	})

	t.Run("remove 2 twice", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5, 6, 7)
		edit(le, 5, 2)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 7}, 1)
		edit(le, 1, 2)
		assertState(t, vrw, le, []int{0, 3, 4, 7}, 2)
	})

	t.Run("null elements", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2)
		le.Append(NullValue)
		le.Append(Float(4))
		l, err := le.List(context.Background())
		require.NoError(t, err)

		v3, err := l.Get(context.Background(), 3)
		require.NoError(t, err)
		assert.True(t, IsNull(v3))
		v4, err := l.Get(context.Background(), 4)
		require.NoError(t, err)
		assert.True(t, v4.Equals(Float(4)))
	})
}

func TestCollapseSplices(t *testing.T) {
	vrw := newTestValueStore()

	t.Run("left adjacent", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5, 6, 7)
		edit(le, 4, 3)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 7}, 1)
		edit(le, 1, 3)
		assertState(t, vrw, le, []int{0, 7}, 1)
	})

	t.Run("left adjacent 2", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5, 6, 7)
		edit(le, 4, 3, 0, 0)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 0, 0, 7}, 1)
		edit(le, 1, 3, 5, 5)
		assertState(t, vrw, le, []int{0, 5, 5, 0, 0, 7}, 1)
	})

	t.Run("left consume", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5, 6, 7)
		edit(le, 2, 4)
		assertState(t, vrw, le, []int{0, 1, 6, 7}, 1)
		edit(le, 1, 2)
		assertState(t, vrw, le, []int{0, 7}, 1)
	})

	t.Run("left overlap ", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 3, 2, 7, 8, 9)
		assertState(t, vrw, le, []int{0, 1, 2, 7, 8, 9, 5}, 1)
		edit(le, 0, 4)
		assertState(t, vrw, le, []int{8, 9, 5}, 1)
	})

	t.Run("undo 1", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 3)
		assertState(t, vrw, le, []int{0, 1, 5}, 1)
		edit(le, 2, 0, 2, 3, 4)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 5}, 1)
	})

	t.Run("undo 2", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 2, 0, 9, 8, 7)
		assertState(t, vrw, le, []int{0, 1, 9, 8, 7, 2, 3, 4, 5}, 1)
		edit(le, 2, 3)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 5}, 0)
	})

	t.Run("splice middile of splice", func(t *testing.T) {
		le := testEditor(vrw, 0, 1)
		edit(le, 1, 0, 9, 8, 7, 6)
		assertState(t, vrw, le, []int{0, 9, 8, 7, 6, 1}, 1)
		edit(le, 2, 2)
		assertState(t, vrw, le, []int{0, 9, 6, 1}, 1)
	})
}

func TestFuzzFails(t *testing.T) {
	vrw := newTestValueStore()

	t.Run("Case 1", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)
		edit(le, 23, 0, 0, 3, 2)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 0, 3, 2, 23, 24}, 1)
		edit(le, 5, 15, 1, 2, 9, 8)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 1, 2, 9, 8, 20, 21, 22, 0, 3, 2, 23, 24}, 2)
		edit(le, 4, 7, 7)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 7, 22, 0, 3, 2, 23, 24}, 2)
	})

	t.Run("Case 2", func(t *testing.T) {
		le := testEditor(vrw, 0, 1, 2, 3, 4, 5)
		edit(le, 5, 0, 1, 7, 5, 3, 13, 17)
		assertState(t, vrw, le, []int{0, 1, 2, 3, 4, 1, 7, 5, 3, 13, 17, 5}, 1)
		edit(le, 2, 2, 16, 5, 12, 5, 15, 0, 15, 15, 7)
		assertState(t, vrw, le, []int{0, 1, 16, 5, 12, 5, 15, 0, 15, 15, 7, 4, 1, 7, 5, 3, 13, 17, 5}, 2)
		edit(le, 8, 5, 4, 13)
		assertState(t, vrw, le, []int{0, 1, 16, 5, 12, 5, 15, 0, 4, 13, 7, 5, 3, 13, 17, 5}, 1)
		edit(le, 6, 2, 8, 2, 6, 3, 14, 6)
		assertState(t, vrw, le, []int{0, 1, 16, 5, 12, 5, 8, 2, 6, 3, 14, 6, 4, 13, 7, 5, 3, 13, 17, 5}, 1)

	})
}

func AsValuables(vs []Value) []Valuable {
	res := make([]Valuable, len(vs))
	for i, v := range vs {
		res[i] = v
	}
	return res
}

func TestListSpliceFuzzer(t *testing.T) {
	startCount := 1000
	rounds := 1000
	splices := 100
	maxInsertCount := uint64(50)
	maxInt := uint64(100)

	vrw := newTestValueStore()

	r := rand.New(rand.NewSource(0))

	nextRandInt := func(from, to uint64) uint64 {
		return from + uint64(float64(to-from)*r.Float64())
	}

	nextRandomSplice := func(len int) (idx, remove uint64, insert []Value) {
		idx = nextRandInt(0, uint64(len))
		remove = nextRandInt(0, uint64(len)-idx)
		insCount := nextRandInt(0, maxInsertCount)
		for i := uint64(0); i < insCount; i++ {
			insert = append(insert, Float(nextRandInt(0, maxInt)))
		}

		return
	}

	for i := 0; i < rounds; i++ {
		tl := newTestList(vrw.Format(), startCount)
		l, err := tl.toList(vrw)
		require.NoError(t, err)
		le := l.Edit()

		for j := 0; j < splices; j++ {
			idx, removed, insert := nextRandomSplice(len(tl))
			tl = tl.Splice(int(idx), int(removed), insert...)
			le.Splice(idx, removed, AsValuables(insert)...)
		}
		expect, err := tl.toList(vrw)
		require.NoError(t, err)
		actual, err := le.List(context.Background())
		require.NoError(t, err)
		assert.True(t, expect.Equals(actual))
	}
}
