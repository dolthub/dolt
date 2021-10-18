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
	"bytes"
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/val"
)

func newTestNRW() NodeReadWriter {
	ts := &chunks.TestStorage{}
	return NewNodeStore(ts.NewView())
}

func randomTree(t *testing.T, count int) (node, [][2]nodeItem, NodeReadWriter) {
	ctx := context.Background()
	nrw := newTestNRW()
	chunker, err := newEmptyTreeChunker(ctx, nrw, newDefaultNodeSplitter)
	require.NoError(t, err)

	items := randomTupleItems(t, count)
	for _, item := range items {
		_, err := chunker.Append(ctx, item[0], item[1])
		assert.NoError(t, err)
	}
	nd, err := chunker.Done(ctx)
	assert.NoError(t, err)
	return nd, items, nrw
}

func randomTupleItems(t *testing.T, count int) (items [][2]nodeItem) {
	fields := (rand.Int() % 20) + 1
	items = make([][2]nodeItem, count/2)
	for i := range items {
		items[i][0] = nodeItem(randomTuple(fields))
		items[i][1] = nodeItem(randomTuple(fields))
		require.Equal(t, 0, compareRandomTuples(items[i][0], items[i][0]))
	}
	sortRandTuples(items)

	for i := range items {
		require.Equal(t, fields, val.Tuple(items[i][0]).Count())
		require.Equal(t, fields, val.Tuple(items[i][1]).Count())
	}

	return
}

func sortRandTuples(items [][2]nodeItem) {
	fields := val.Tuple(items[0][0]).Count()
	types := make([]val.Type, fields)
	for i := range types {
		types[i] = val.Type{
			Coll:     val.ByteOrderCollation,
			Nullable: true,
		}
	}

	td := val.NewTupleDescriptor(types...)

	sort.Slice(items, func(i, j int) bool {
		l, r := val.Tuple(items[i][0]), val.Tuple(items[j][0])
		return td.Compare(l, r) == -1
	})
}

func compareRandomTuples(left, right nodeItem) (cmp int) {
	l, r := val.Tuple(left), val.Tuple(right)

	cnt := l.Count()
	if r.Count() < l.Count() {
		cnt = r.Count()
		if l.Count()-r.Count() != 2 {
			// meta tuples are length + 2
			panic("")
		}
	}

	for i := 0; i < cnt; i++ {
		cmp = bytes.Compare(l.GetField(i), r.GetField(i))
		if cmp != 0 {
			break
		}
	}

	return cmp
}

func randomTuple(fields int) val.Tuple {
	vals := make([][]byte, fields)
	for i := range vals {
		vals[i] = randomVal()

	}
	return val.NewTuple(shared, vals...)
}

func randomVal() (v []byte) {
	x := rand.Int()
	if x%4 == 0 {
		return nil // 25% NULL
	}
	v = make([]byte, x%20)
	rand.Read(v)
	return
}
