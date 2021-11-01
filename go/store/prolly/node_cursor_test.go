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

package prolly

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/val"
)

func TestNodeCursor(t *testing.T) {
	t.Run("new cursor at item", func(t *testing.T) {
		testNewCursorAtItem(t, 10)
		testNewCursorAtItem(t, 100)
		testNewCursorAtItem(t, 1000)
	})
}

func testNewCursorAtItem(t *testing.T, count int) {
	root, items, nrw := randomTree(t, count)
	assert.NotNil(t, root)

	ctx := context.Background()
	for i := range items {
		key, value := items[i][0], items[i][1]
		cur, err := newCursorAtItem(ctx, nrw, root, key, searchTestTree)
		require.NoError(t, err)
		assert.Equal(t, key, cur.current())
		_, err = cur.advance(ctx)
		require.NoError(t, err)
		assert.Equal(t, value, cur.current())
	}

	validateTreeItems(t, nrw, root, items)
}

func TestTreeCursor(t *testing.T) {
	t.Run("tree cursor", func(t *testing.T) {
		testTreeCursor(t, 10)
		testTreeCursor(t, 100)
		testTreeCursor(t, 1000)
	})
}

func testTreeCursor(t *testing.T, count int) {
	root, items, nrw := randomTree(t, count)
	assert.NotNil(t, root)

	ctx := context.Background()
	tc, err := newTreeCursor(ctx, nrw, root)
	require.NoError(t, err)
	for _, item := range items {
		assert.Equal(t, item[0], tc.current())
		_, err = tc.advance(ctx)
		require.NoError(t, err)

		assert.Equal(t, item[1], tc.current())
		_, err = tc.advance(ctx)
		require.NoError(t, err)
	}
}

func newTestNRW() NodeStore {
	ts := &chunks.TestStorage{}
	return NewNodeStore(ts.NewView())
}

func randomTree(t *testing.T, count int) (Node, [][2]nodeItem, NodeStore) {
	ctx := context.Background()
	nrw := newTestNRW()
	chunker, err := newEmptyTreeChunker(ctx, nrw, newDefaultNodeSplitter)
	require.NoError(t, err)

	items := randomTupleItemPairs(count)
	for _, item := range items {
		_, err := chunker.Append(ctx, item[0], item[1])
		assert.NoError(t, err)
	}
	nd, err := chunker.Done(ctx)
	assert.NoError(t, err)
	return nd, items, nrw
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

func searchTestTree(item nodeItem, nd Node) int {
	card := 1
	if nd.leafNode() {
		card = 2
	}

	idx := sort.Search(nd.nodeCount()/card, func(i int) bool {
		l, r := val.Tuple(item), val.Tuple(nd.getItem(i*card))
		return keyDesc.Compare(l, r) <= 0
	})

	return idx * card
}

func randomTupleItemPairs(count int) (items [][2]nodeItem) {
	tups := randomTuplePairs(count, keyDesc, valDesc)
	items = make([][2]nodeItem, count/2)
	if len(tups) != len(items) {
		panic("mismatch")
	}

	for i := range items {
		items[i][0] = nodeItem(tups[i][0])
		items[i][1] = nodeItem(tups[i][1])
	}
	return
}
