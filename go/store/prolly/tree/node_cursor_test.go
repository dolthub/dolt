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
	"sort"
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
}

func testNewCursorAtItem(t *testing.T, count int) {
	root, items, ns := randomTree(t, count)
	assert.NotNil(t, root)

	ctx := context.Background()
	for i := range items {
		key, value := items[i][0], items[i][1]
		cur, err := NewCursorAtItem(ctx, ns, root, key, searchTestTree)
		require.NoError(t, err)
		assert.Equal(t, key, cur.CurrentKey())
		assert.Equal(t, value, cur.CurrentValue())
	}

	validateTreeItems(t, ns, root, items)
}

func randomTree(t *testing.T, count int) (Node, [][2]Item, NodeStore) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	items := randomTupleItemPairs(count / 2)
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

func searchTestTree(item Item, nd Node) int {
	return sort.Search(int(nd.count), func(i int) bool {
		l, r := val.Tuple(item), val.Tuple(nd.GetKey(i))
		return keyDesc.Compare(l, r) <= 0
	})
}

func randomTupleItemPairs(count int) (items [][2]Item) {
	tups := RandomTuplePairs(count, keyDesc, valDesc)
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
