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
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripInts(t *testing.T) {
	keys, values := ascendingIntPairs(t, 10)
	require.True(t, sumSize(keys)+sumSize(values) < maxNodeDataSize)

	nd := newLeafNode(keys, values)
	assert.True(t, nd.leafNode())
	assert.Equal(t, len(keys), nd.nodeCount())
	for i := range keys {
		assert.Equal(t, keys[i], nd.getKey(i))
		assert.Equal(t, values[i], nd.getValue(i))
	}
}

func TestRoundTripNodeItems(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
		require.True(t, sumSize(keys)+sumSize(values) < maxNodeDataSize)

		nd := newLeafNode(keys, values)
		assert.True(t, nd.leafNode())
		assert.Equal(t, len(keys), nd.nodeCount())
		for i := range keys {
			assert.Equal(t, keys[i], nd.getKey(i))
			assert.Equal(t, values[i], nd.getValue(i))
		}
	}
}

func newLeafNode(keys, values []nodeItem) Node {
	return makeMapNode(sharedPool, 0, keys, values)
}

func randomNodeItemPairs(t *testing.T, count int) (keys, values []nodeItem) {
	keys = make([]nodeItem, count)
	for i := range keys {
		sz := (rand.Int() % 41) + 10
		keys[i] = make(nodeItem, sz)
		_, err := rand.Read(keys[i])
		assert.NoError(t, err)
	}

	values = make([]nodeItem, count)
	copy(values, keys)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	return
}

func ascendingIntPairs(t *testing.T, count int) (keys, values []nodeItem) {
	items := make([]nodeItem, count*2)
	for i := range items {
		items[i] = make(nodeItem, 4)
		binary.LittleEndian.PutUint32(items[i], uint32(i))
	}
	keys, values = items[:count], items[count:]
	return
}

func sumSize(items []nodeItem) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}
