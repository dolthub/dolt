// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

// TestMutableMapVisitGCRoots tests that a MutableMap visits what its root references rather than its root, which
// needn't be a chunk in the store.
func TestMutableMapVisitGCRoots(t *testing.T) {
	ctx := context.Background()
	visit := func(t *testing.T, mut *MutableMap) hash.HashSet {
		visited := hash.NewHashSet()
		require.NoError(t, mut.VisitGCRoots(ctx, func(h hash.Hash) {
			visited.Insert(h)
		}))
		return visited
	}

	t.Run("internal root", func(t *testing.T) {
		m := ascendingIntMap(t, 100_000)
		require.False(t, m.Node().IsLeaf())
		children := hash.NewHashSet()
		require.NoError(t, tree.WalkNodeAddresses(ctx, m.Node(), func(_ context.Context, h hash.Hash) error {
			children.Insert(h)
			return nil
		}))
		require.NotEmpty(t, children)

		mut := m.Mutate()
		k, v := makePut(1, 2)
		require.NoError(t, mut.Put(ctx, k, v))
		visited := visit(t, mut)
		assert.Equal(t, children, visited)
		assert.False(t, visited.Has(m.HashOf()))
	})

	t.Run("leaf root without addresses", func(t *testing.T) {
		m := ascendingIntMap(t, 10)
		require.True(t, m.Node().IsLeaf())
		assert.Empty(t, visit(t, m.Mutate()))
	})
}
