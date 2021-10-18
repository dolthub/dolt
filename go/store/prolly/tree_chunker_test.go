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
	"math/rand"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	fields := (rand.Int() % 20) + 1
	root, _, _ := randomTree(t, 1000, fields)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
}

func roundTripTreeItems(t *testing.T) {
	fields := (rand.Int() % 20) + 1
	root, items, nrw := randomTree(t, 1000, fields)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, uint64(1000), root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 1000)
	validateTreeItems(t, nrw, root, items)

	root, items, nrw = randomTree(t, 10_000, fields)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, uint64(10_000), root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 10_000)
	validateTreeItems(t, nrw, root, items)

	root, items, nrw = randomTree(t, 100_000, fields)
	assert.NotNil(t, root)
	assert.True(t, root.nodeCount() > 0)
	assert.True(t, root.level() > 0)
	assert.Equal(t, uint64(100_000), root.cumulativeCount())
	assert.Equal(t, countTree(t, nrw, root), 100_000)
	validateTreeItems(t, nrw, root, items)
}

func roundTripMetaTupleFields(t *testing.T) {
	vals := [][]byte{{0}}

	cnt := uint64(rand.Uint32() & 8096)
	ref := hash.Hash{}
	rand.Read(ref[:])

	meta := newMetaTuple(shared, cnt, ref, vals)
	assert.Equal(t, cnt, meta.GetCumulativeCount())
	//assert.Equal(t, cnt, meta.GetRef())
}

func countTree(t *testing.T, nrw NodeReadWriter, nd Node) (count int) {
	ctx := context.Background()
	err := iterTree(ctx, nrw, nd, func(_ nodeItem) (err error) {
		count++
		return
	})
	require.NoError(t, err)
	return
}

func validateTreeItems(t *testing.T, nrw NodeReadWriter, nd Node, expected [][2]nodeItem) {
	i := 0
	ctx := context.Background()
	err := iterTree(ctx, nrw, nd, func(actual nodeItem) (err error) {
		if !assert.Equal(t, expected[i/2][i%2], actual) {
			panic("here")
		}
		i++
		return
	})
	require.NoError(t, err)
	return
}
