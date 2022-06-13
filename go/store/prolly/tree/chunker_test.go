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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTreeChunker(t *testing.T) {
	t.Run("round trip tree items", func(t *testing.T) {
		roundTripTreeItems(t)
	})
}

func roundTripTreeItems(t *testing.T) {
	root, items, ns := randomTree(t, 1000)
	assert.NotNil(t, root)
	assert.True(t, root.count > 0)
	assert.True(t, root.Level() > 0)
	//assert.Equal(t, uint64(1000), root.cumulativeCount())
	assert.Equal(t, countTree(t, ns, root), 1000)
	assert.Equal(t, root.TreeCount()*2, 1000)
	validateTreeItems(t, ns, root, items)

	root, items, ns = randomTree(t, 10_000)
	assert.NotNil(t, root)
	assert.True(t, root.count > 0)
	assert.True(t, root.Level() > 0)
	//assert.Equal(t, uint64(10_000), root.cumulativeCount())
	assert.Equal(t, countTree(t, ns, root), 10_000)
	assert.Equal(t, root.TreeCount()*2, 10_000)
	validateTreeItems(t, ns, root, items)

	root, items, ns = randomTree(t, 100_000)
	assert.NotNil(t, root)
	assert.True(t, root.count > 0)
	assert.True(t, root.Level() > 0)
	//assert.Equal(t, uint64(100_000), root.cumulativeCount())
	assert.Equal(t, countTree(t, ns, root), 100_000)
	assert.Equal(t, root.TreeCount()*2, 100_000)
	validateTreeItems(t, ns, root, items)
}

func countTree(t *testing.T, ns NodeStore, nd Node) (count int) {
	ctx := context.Background()
	err := iterTree(ctx, ns, nd, func(_ Item) (err error) {
		count++
		return
	})
	require.NoError(t, err)
	return
}

func validateTreeItems(t *testing.T, ns NodeStore, nd Node, expected [][2]Item) {
	i := 0
	ctx := context.Background()
	err := iterTree(ctx, ns, nd, func(actual Item) (err error) {
		assert.Equal(t, expected[i/2][i%2], actual)
		i++
		return
	})
	require.NoError(t, err)
	return
}

func iterTree(ctx context.Context, ns NodeStore, nd Node, cb func(item Item) error) error {
	if nd.empty() {
		return nil
	}

	cur, err := NewCursorAtStart(ctx, ns, nd)
	if err != nil {
		return err
	}

	for !cur.outOfBounds() {
		err = cb(cur.CurrentKey())
		if err != nil {
			return err
		}

		err = cb(cur.CurrentValue())
		if err != nil {
			return err
		}

		err = cur.Advance(ctx)
		if err != nil {
			return err
		}
	}
	return err
}
