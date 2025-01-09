// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestGhostBlockStore(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	bs, err := NewGhostBlockStore(path)
	require.NoError(t, err)
	ghost, absent := hash.Parse("ifho8m890r9787lrpthif5ce6ru353fr"), hash.Parse("6af71afc2ea0hmp4olev0vp9q1q5gvb1")
	require.NoError(t, bs.PersistGhostHashes(context.Background(), hash.NewHashSet(ghost)))
	t.Run("Get", func(t *testing.T) {
		t.Run("Ghost", func(t *testing.T) {
			c, err := bs.Get(ctx, ghost)
			require.NoError(t, err)
			require.True(t, c.IsGhost())
		})
		t.Run("Absent", func(t *testing.T) {
			c, err := bs.Get(ctx, absent)
			require.NoError(t, err)
			require.False(t, c.IsGhost())
			require.True(t, c.IsEmpty())
		})
	})
	t.Run("Has", func(t *testing.T) {
		t.Run("Ghost", func(t *testing.T) {
			h, err := bs.Has(ctx, ghost)
			require.NoError(t, err)
			require.True(t, h)
		})
		t.Run("Absent", func(t *testing.T) {
			h, err := bs.Has(ctx, absent)
			require.NoError(t, err)
			require.False(t, h)
		})
	})
	t.Run("HasMany", func(t *testing.T) {
		a, err := bs.HasMany(ctx, hash.NewHashSet(absent, ghost))
		require.NoError(t, err)
		require.Len(t, a, 1)
		require.True(t, a.Has(absent))
		require.False(t, a.Has(ghost))
	})
	t.Run("GetMany", func(t *testing.T) {
		var got []chunks.Chunk
		err := bs.GetMany(ctx, hash.NewHashSet(absent, ghost), func(_ context.Context, c *chunks.Chunk) {
			got = append(got, *c)
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.True(t, got[0].IsGhost())
		require.Equal(t, ghost, got[0].Hash())
	})
	t.Run("GetManyCompressed", func(t *testing.T) {
		var got []ToChunker
		err := bs.GetManyCompressed(ctx, hash.NewHashSet(absent, ghost), func(_ context.Context, c ToChunker) {
			got = append(got, c)
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.True(t, got[0].IsGhost())
		require.Equal(t, ghost, got[0].Hash())
	})
}
