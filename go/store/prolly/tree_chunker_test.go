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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"context"
	"github.com/dolthub/dolt/go/store/hash"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/val"
)

func TestTreeChunker(t *testing.T) {
	t.Run("smoke test tree chunker", func(t *testing.T) {
		smokeTestTreeChunker(t)
	})
	t.Run("round trip tree items", func(t *testing.T) {
		roundTripTreeItems(t)
	})
}

func TestMetaTuple(t *testing.T) {
	t.Run("round trip meta tuple fields", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			roundTripMetaTupleFields(t)
		}
	})
}

func smokeTestTreeChunker(t *testing.T) {
	root, _, _ := randomTree(t, 1000)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
}

func roundTripTreeItems(t *testing.T) {
	root, items, nrw := randomTree(t, 1000)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, 1000, root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 1000)
	validateTreeItems(t, nrw, root, items)

	root, items, nrw = randomTree(t, 10_000)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, 10_000, root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 10_000)
	validateTreeItems(t, nrw, root, items)

	root, items, nrw = randomTree(t, 100_000)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, 100_000, root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 100_000)
	validateTreeItems(t, nrw, root, items)
}

func countTree(t *testing.T, nrw NodeReadWriter, nd node) (count int) {
	ctx := context.Background()
	err := iterTree(ctx, nrw, nd, func(_ nodeItem) (err error) {
		count++
		return
	})
	require.NoError(t, err)
	return
}

func validateTreeItems(t *testing.T, nrw NodeReadWriter, nd node, expected []nodeItem) {
	i := 0
	ctx := context.Background()
	err := iterTree(ctx, nrw, nd, func(actual nodeItem) (err error) {
		assert.Equal(t, expected[i], actual)
		i++
		return
	})
	require.NoError(t, err)
	return
}


func roundTripMetaTupleFields(t *testing.T) {
	vals := [][]byte{{0}}

	cnt := uint64(rand.Uint32()&8096)
	ref := hash.Hash{}
	rand.Read(ref[:])

	meta := newMetaTuple(shared, cnt, ref, vals)
	assert.Equal(t, cnt, meta.GetCumulativeCount())
	//assert.Equal(t, cnt, meta.GetRef())
}

func newTestNRW() NodeReadWriter {
	ts := &chunks.TestStorage{}
	return NewNodeStore(ts.NewView())
}

func randomTree(t *testing.T, count int) (node, []nodeItem, NodeReadWriter) {
	ctx := context.Background()
	nrw := newTestNRW()
	chunker, err := newEmptyTreeChunker(ctx, nrw, newDefaultNodeSplitter)
	require.NoError(t, err)

	items := randomTupleItems(count)
	for i := 0; i < len(items); i += 2 {
		_, err := chunker.Append(ctx, items[i], items[i+1])
		assert.NoError(t, err)
	}
	nd, err := chunker.Done(ctx)
	assert.NoError(t, err)
	return nd, items, nrw
}

func randomTupleItems(count int) (items []nodeItem) {
	fields := (rand.Int() % 20) + 1
	items = make([]nodeItem, count)
	for i := range items {
		items[i] = nodeItem(randomTuple(fields))
	}
	return
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
