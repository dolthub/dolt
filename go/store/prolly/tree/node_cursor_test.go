// Copyright 2021 Dolthub, Inc.
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

package tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
)

func TestNodeCursor(t *testing.T) {
	t.Run("new cursor at item", func(t *testing.T) {
		testNewCursorAtItem(t, 10)
		testNewCursorAtItem(t, 100)
		testNewCursorAtItem(t, 1000)
		testNewCursorAtItem(t, 10_000)
	})

	t.Run("get ordinal at item", func(t *testing.T) {
		counts := []int{10, 100, 1000, 10_000}
		for _, c := range counts {
			t.Run(fmt.Sprintf("%d", c), func(t *testing.T) {
				testGetOrdinalOfCursor(t, c)
			})
		}
	})

	t.Run("retreat past beginning", func(t *testing.T) {
		ctx := context.Background()
		root, _, ns := randomTree(t, 10_000)
		assert.NotNil(t, root)
		before, err := newCursorAtStart(ctx, ns, root)
		assert.NoError(t, err)
		err = before.retreat(ctx)
		assert.NoError(t, err)
		assert.False(t, before.Valid())

		start, err := newCursorAtStart(ctx, ns, root)
		assert.NoError(t, err)
		assert.True(t, start.compare(before) > 0, "start is after before")
		assert.True(t, before.compare(start) < 0, "before is before start")

		// Backwards iteration...
		end, err := newCursorAtEnd(ctx, ns, root)
		assert.NoError(t, err)
		i := 0
		for end.compare(before) > 0 {
			i++
			err = end.retreat(ctx)
			assert.NoError(t, err)
		}
		assert.Equal(t, 10_000/2, i)
	})
}

func testNewCursorAtItem(t *testing.T, count int) {
	root, items, ns := randomTree(t, count)
	assert.NotNil(t, root)

	ctx := context.Background()
	for i := range items {
		key, value := items[i][0], items[i][1]
		cur, err := newCursorAtKey(ctx, ns, root, val.Tuple(key), keyDesc)
		require.NoError(t, err)
		assert.Equal(t, key, cur.CurrentKey())
		assert.Equal(t, value, cur.currentValue())
	}

	validateTreeItems(t, ns, root, items)
}

func testGetOrdinalOfCursor(t *testing.T, count int) {
	tuples, desc := AscendingUintTuples(count)

	ctx := context.Background()
	ns := NewTestNodeStore()
	serializer := message.NewProllyMapSerializer(desc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, item := range tuples {
		err = chkr.AddPair(ctx, Item(item[0]), Item(item[1]))
		assert.NoError(t, err)
	}
	nd, err := chkr.Done(ctx)
	assert.NoError(t, err)

	for i := 0; i < len(tuples); i++ {
		curr, err := newCursorAtKey(ctx, ns, nd, tuples[i][0], desc)
		require.NoError(t, err)

		ord, err := getOrdinalOfCursor(curr)
		require.NoError(t, err)

		assert.Equal(t, uint64(i), ord)
	}

	b := val.NewTupleBuilder(desc)
	b.PutUint32(0, uint32(len(tuples)))
	aboveItem := b.Build(sharedPool)

	curr, err := newCursorAtKey(ctx, ns, nd, aboveItem, desc)
	require.NoError(t, err)

	ord, err := getOrdinalOfCursor(curr)
	require.NoError(t, err)

	require.Equal(t, uint64(len(tuples)), ord)

	// A cursor past the end should return an ordinal count equal to number of
	// nodes.
	curr, err = newCursorPastEnd(ctx, ns, nd)
	require.NoError(t, err)

	ord, err = getOrdinalOfCursor(curr)
	require.NoError(t, err)

	require.Equal(t, uint64(len(tuples)), ord)
}

func randomTree(t *testing.T, count int) (Node, [][2]Item, NodeStore) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	serializer := message.NewProllyMapSerializer(valDesc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	items := randomTupleItemPairs(count/2, ns)
	for _, item := range items {
		err = chkr.AddPair(ctx, Item(item[0]), Item(item[1]))
		assert.NoError(t, err)
	}
	nd, err := chkr.Done(ctx)
	assert.NoError(t, err)
	return nd, items, ns
}

var keyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: false},
)
var valDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

func randomTupleItemPairs(count int, ns NodeStore) (items [][2]Item) {
	tups := RandomTuplePairs(ctx, count, keyDesc, valDesc, ns)
	items = make([][2]Item, count)
	if len(tups) != len(items) {
		panic("mismatch")
	}

	for i := range items {
		items[i][0] = Item(tups[i][0])
		items[i][1] = Item(tups[i][1])
	}
	return
}
