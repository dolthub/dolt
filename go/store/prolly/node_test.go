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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

func TestRoundTripNodeItems(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		items := randomNodeItems(t, (rand.Int()%101)+50)
		// sanity check
		require.True(t, sumSize(items) < maxNodeDataSize)

		nd := newLeafNode(items)
		assert.True(t, nd.leafNode())
		assert.Equal(t, len(items), nd.nodeCount())
		for i, exp := range items {
			assert.Equal(t, exp, nd.getItem(i))
		}
	}
}

func newLeafNode(items []nodeItem) node {
	return makeProllyNode(shared, 0, items...)
}

var shared = pool.NewBuffPool()

func randomNodeItems(t *testing.T, count int) (items []nodeItem) {
	items = make([]nodeItem, count)
	for i := range items {
		sz := (rand.Int() % 41) + 10
		items[i] = make(nodeItem, sz)
		_, err := rand.Read(items[i])
		assert.NoError(t, err)
	}
	return
}

func sumSize(items []nodeItem) (sz val.ByteSize) {
	for _, item := range items {
		sz += val.ByteSize(len(item))
	}
	return
}
