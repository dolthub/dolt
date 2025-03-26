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
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/assert"
)

func TestPullChunkTracker(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: nil,
		})
		eg.Go(func() error {
			return tracker.Run(ctx, make(hash.HashSet))
		})
		eg.Go(func() error {
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 0)
			assert.False(t, ok)
			assert.NoError(t, err)
			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("HasAllInitial", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: hasAllHaser{},
		})
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 0)
			assert.False(t, ok)
			assert.NoError(t, err)
			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("HasNoneInitial", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		hs := make(hash.HashSet)
		for i := byte(1); i <= byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: hasNoneHaser{},
		})
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 10)
			assert.True(t, ok)
			assert.NoError(t, err)
			for _ = range hs {
				tracker.TickProcessed(ctx)
			}
			hs, ok, err = tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 0)
			assert.False(t, ok)
			assert.NoError(t, err)

			for i := byte(1); i <= byte(10); i++ {
				var h hash.Hash
				h[1] = i
				tracker.Seen(ctx, h)
			}

			cnt := 0
			for {
				hs, ok, err := tracker.GetChunksToFetch(ctx)
				assert.NoError(t, err)
				if !ok {
					assert.Equal(t, 10, cnt)
					break
				}
				cnt += len(hs)
				for _ = range hs {
					tracker.TickProcessed(ctx)
				}
			}

			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("HasManyError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: errHaser{},
		})
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			_, _, err := tracker.GetChunksToFetch(ctx)
			assert.Error(t, err)
			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("InitialAreSeen", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		hs := make(hash.HashSet)
		for i := byte(0); i < byte(10); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: hasNoneHaser{},
		})
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 10)
			assert.True(t, ok)
			assert.NoError(t, err)

			for i := byte(0); i < byte(10); i++ {
				var h hash.Hash
				h[0] = i
				tracker.Seen(ctx, h)
			}
			for _ = range hs {
				tracker.TickProcessed(ctx)
			}

			hs, ok, err = tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 0)
			assert.False(t, ok)
			assert.NoError(t, err)

			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("StaticHaser", func(t *testing.T) {
		haser := staticHaser{make(hash.HashSet)}
		initial := make([]hash.Hash, 4)
		initial[0][0] = 1
		initial[1][0] = 2
		initial[2][0] = 1
		initial[2][1] = 1
		initial[3][0] = 1
		initial[3][1] = 2
		haser.has.Insert(initial[0])
		haser.has.Insert(initial[1])
		haser.has.Insert(initial[2])
		haser.has.Insert(initial[3])

		hs := make(hash.HashSet)
		// Start with 1 - 5
		for i := byte(1); i <= byte(5); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 64 * 1024,
			HasManyer: haser,
		})
		
		eg, ctx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			// Should get back 03, 04, 05
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 3)
			assert.True(t, ok)
			assert.NoError(t, err)
			for _ = range hs {
				tracker.TickProcessed(ctx)
			}

			for i := byte(1); i <= byte(10); i++ {
				var h hash.Hash
				h[0] = 1
				h[1] = i
				tracker.Seen(ctx, h)
			}

			// Should get back 13, 14, 15, 16, 17, 18, 19, 1(10).
			cnt := 0
			for {
				hs, ok, err := tracker.GetChunksToFetch(ctx)
				assert.NoError(t, err)
				if !ok {
					break
				}
				cnt += len(hs)
				for _ = range hs {
					tracker.TickProcessed(ctx)
				}
			}
			assert.Equal(t, 8, cnt)

			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
	})

	t.Run("SmallBatches", func(t *testing.T) {
		haser := staticHaser{make(hash.HashSet)}
		initial := make([]hash.Hash, 4)
		initial[0][0] = 1
		initial[1][0] = 2
		initial[2][0] = 1
		initial[2][1] = 1
		initial[3][0] = 1
		initial[3][1] = 2
		haser.has.Insert(initial[0])
		haser.has.Insert(initial[1])
		haser.has.Insert(initial[2])
		haser.has.Insert(initial[3])

		hs := make(hash.HashSet)
		// Start with 1 - 5
		for i := byte(1); i <= byte(5); i++ {
			var h hash.Hash
			h[0] = i
			hs.Insert(h)
		}
		tracker := NewPullChunkTracker(TrackerConfig{
			BatchSize: 1,
			HasManyer: haser,
		})

		eg, ctx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return tracker.Run(ctx, hs)
		})
		eg.Go(func() error {
			// First call doesn't actually respect batch size.
			hs, ok, err := tracker.GetChunksToFetch(ctx)
			assert.Len(t, hs, 3)
			assert.True(t, ok)
			assert.NoError(t, err)
			for _ = range hs {
				tracker.TickProcessed(ctx)
			}

			for i := byte(1); i <= byte(10); i++ {
				var h hash.Hash
				h[0] = 1
				h[1] = i
				tracker.Seen(ctx, h)
			}

			// Should get back 13, 14, 15, 16, 17, 18, 19, 1(10); one at a time.
			cnt := 0
			for {
				hs, ok, err := tracker.GetChunksToFetch(ctx)
				assert.NoError(t, err)
				if !ok {
					break
				}
				assert.Len(t, hs, 1)
				cnt += len(hs)
				for _ = range hs {
					tracker.TickProcessed(ctx)
				}
			}
			assert.Equal(t, 8, cnt)

			tracker.Close()
			return nil
		})
		assert.NoError(t, eg.Wait())
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

type staticHaser struct {
	has hash.HashSet
}

func (s staticHaser) HasMany(ctx context.Context, query hash.HashSet) (hash.HashSet, error) {
	ret := make(hash.HashSet)
	for h := range query {
		if !s.has.Has(h) {
			ret.Insert(h)
		}
	}
	return ret, nil
}

type errHaser struct {
}

func (errHaser) HasMany(ctx context.Context, hs hash.HashSet) (hash.HashSet, error) {
	return nil, errors.New("always throws an error")
}
