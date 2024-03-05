// Copyright 2024 Dolthub, Inc.
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

package pull

import (
	"context"
	"errors"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/stretchr/testify/assert"
)

func TestPullChunkTracker(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		tracker := NewPullChunkTracker(context.Background(), make(hash.HashSet), TrackerConfig{
			BatchSize:          64 * 1024,
			HasManyer:          nil,
		})
		hs, ok, err := tracker.GetChunksToFetch()
		assert.Len(t, hs, 0)
		assert.False(t, ok)
		assert.NoError(t, err)
		tracker.Close()
	})

	t.Run("HasAllInitial", func(t *testing.T) {
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(context.Background(), hs, TrackerConfig{
			BatchSize:          64 * 1024,
			HasManyer:          hasAllHaser{},
		})
		hs, ok, err := tracker.GetChunksToFetch()
		assert.Len(t, hs, 0)
		assert.False(t, ok)
		assert.NoError(t, err)
		tracker.Close()
	})

	t.Run("HasNoneInitial", func(t *testing.T) {
		hs := make(hash.HashSet)
		for i := byte(1); i <= byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(context.Background(), hs, TrackerConfig{
			BatchSize:          64 * 1024,
			HasManyer:          hasNoneHaser{},
		})
		hs, ok, err := tracker.GetChunksToFetch()
		assert.Len(t, hs, 10)
		assert.True(t, ok)
		assert.NoError(t, err)
		hs, ok, err = tracker.GetChunksToFetch()
		assert.Len(t, hs, 0)
		assert.False(t, ok)
		assert.NoError(t, err)

		for i := byte(1); i <= byte(10); i++ {
			var h hash.Hash
			h[1] = i
			tracker.Seen(h)
		}

		cnt := 0
		for {
			hs, ok, err := tracker.GetChunksToFetch()
			assert.NoError(t, err)
			if !ok {
				assert.Equal(t, 10, cnt)
				break
			}
			cnt += len(hs)
		}

		tracker.Close()
	})

	t.Run("HasManyError", func(t *testing.T) {
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(context.Background(), hs, TrackerConfig{
			BatchSize:          64 * 1024,
			HasManyer:          errHaser{},
		})
		_, _, err := tracker.GetChunksToFetch()
		assert.Error(t, err)
		tracker.Close()
	})

	t.Run("InitialAreSeen", func(t *testing.T) {
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(context.Background(), hs, TrackerConfig{
			BatchSize:          64 * 1024,
			HasManyer:          hasNoneHaser{},
		})
		hs, ok, err := tracker.GetChunksToFetch()
		assert.Len(t, hs, 10)
		assert.True(t, ok)
		assert.NoError(t, err)

		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			tracker.Seen(h)
		}

		hs, ok, err = tracker.GetChunksToFetch()
		assert.Len(t, hs, 0)
		assert.False(t, ok)
		assert.NoError(t, err)

		tracker.Close()
	})
}

type hasAllHaser struct {
}

func (hasAllHaser) HasMany(context.Context, hash.HashSet) (hash.HashSet, error) {
	return make(hash.HashSet), nil
}

type hasNoneHaser struct {
}

func (hasNoneHaser) HasMany(ctx context.Context, hs hash.HashSet) (hash.HashSet, error) {
	return hs, nil
}

type errHaser struct {
}

func (errHaser) HasMany(ctx context.Context, hs hash.HashSet) (hash.HashSet, error) {
	return nil, errors.New("always throws an error")
}
