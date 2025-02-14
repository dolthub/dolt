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

package types

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestValueReadWriteRead(t *testing.T) {
	assert := assert.New(t)

	s := String("hello")
	vs := newTestValueStore()
	vs.skipWriteCaching = true
	assert.Nil(vs.ReadValue(context.Background(), mustHash(s.Hash(vs.Format())))) // nil
	h := mustRef(vs.WriteValue(context.Background(), s)).TargetHash()
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
	v, err := vs.ReadValue(context.Background(), h) // non-nil
	require.NoError(t, err)
	if assert.NotNil(v) {
		assert.True(s.Equals(v), "%s != %s", mustString(EncodedValue(context.Background(), s)), mustString(EncodedValue(context.Background(), v)))
	}
}

func TestReadWriteCache(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ts := storage.NewView()
	vs := NewValueStore(ts)
	vs.skipWriteCaching = true

	var v Value = Bool(true)
	r, err := vs.WriteValue(context.Background(), v)
	require.NoError(t, err)
	assert.NotEqual(hash.Hash{}, r.TargetHash())
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
	assert.Equal(1, ts.Writes())

	v, err = vs.ReadValue(context.Background(), r.TargetHash())
	require.NoError(t, err)
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads())

	v, err = vs.ReadValue(context.Background(), r.TargetHash())
	require.NoError(t, err)
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads())
}

func TestValueReadMany(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Float(42)}
	vs := newTestValueStore()
	vs.skipWriteCaching = true
	hashes := hash.HashSlice{}
	for _, v := range vals {
		h := mustRef(vs.WriteValue(context.Background(), v)).TargetHash()
		hashes = append(hashes, h)
		rt, err := vs.Root(context.Background())
		require.NoError(t, err)
		_, err = vs.Commit(context.Background(), rt, rt)
		require.NoError(t, err)
	}

	// Get one Value into vs's Value cache
	_, err := vs.ReadValue(context.Background(), mustHash(vals[0].Hash(vs.Format())))
	require.NoError(t, err)

	// Get one Value into vs's pendingPuts
	three := Float(3)
	vals = append(vals, three)
	_, err = vs.WriteValue(context.Background(), three)
	require.NoError(t, err)
	hashes = append(hashes, mustHash(three.Hash(vs.Format())))

	// Add one Value to request that's not in vs
	hashes = append(hashes, mustHash(Bool(false).Hash(vs.Format())))

	found := map[hash.Hash]Value{}
	readValues, err := vs.ReadManyValues(context.Background(), hashes)
	require.NoError(t, err)

	for i, v := range readValues {
		if v != nil {
			found[hashes[i]] = v
		}
	}

	assert.Len(found, len(vals))
	for _, v := range vals {
		assert.True(v.Equals(found[mustHash(v.Hash(vs.Format()))]))
	}
}

func TestPanicOnBadVersion(t *testing.T) {
	storage := &chunks.MemoryStorage{}
	t.Run("Read", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() {
			cvs.ReadValue(context.Background(), hash.Hash{})
		})
	})
	t.Run("Write", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() {
			b, err := NewEmptyBlob(cvs)
			require.NoError(t, err)
			_, err = cvs.WriteValue(context.Background(), b)
			require.NoError(t, err)

			rt, err := cvs.Root(context.Background())
			require.NoError(t, err)
			_, err = cvs.Commit(context.Background(), rt, rt)
			require.NoError(t, err)
		})
	})
}

func TestErrorIfDangling(t *testing.T) {
	t.Skip("WriteValue errors with dangling ref error")
	vs := newTestValueStore()
	vs.skipWriteCaching = true

	r, err := NewRef(Bool(true), vs.Format())
	require.NoError(t, err)
	l, err := NewList(context.Background(), vs, r)
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), l)
	require.NoError(t, err)

	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.Error(t, err)
}

func TestGC(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	vs := newTestValueStore()
	vs.skipWriteCaching = true
	r1 := mustRef(vs.WriteValue(ctx, String("committed")))
	r2 := mustRef(vs.WriteValue(ctx, String("unreferenced")))
	set1 := mustSet(NewSet(ctx, vs, r1))
	set2 := mustSet(NewSet(ctx, vs, r2))

	h1 := mustRef(vs.WriteValue(ctx, set1)).TargetHash()

	rt, err := vs.Root(ctx)
	require.NoError(t, err)
	ok, err := vs.Commit(ctx, h1, rt)
	require.NoError(t, err)
	assert.True(ok)
	h2 := mustRef(vs.WriteValue(ctx, set2)).TargetHash()

	ok, err = vs.Commit(ctx, h1, h1)
	require.NoError(t, err)
	assert.True(ok)

	v1, err := vs.ReadValue(ctx, h1) // non-nil
	require.NoError(t, err)
	assert.NotNil(v1)
	v2, err := vs.ReadValue(ctx, h2) // non-nil
	require.NoError(t, err)
	assert.NotNil(v2)

	err = vs.GC(ctx, GCModeDefault, hash.HashSet{}, hash.HashSet{}, purgingSafepointController{vs})
	require.NoError(t, err)

	v1, err = vs.ReadValue(ctx, h1) // non-nil
	require.NoError(t, err)
	assert.NotNil(v1)
	v2, err = vs.ReadValue(ctx, h2) // nil
	require.NoError(t, err)
	assert.Nil(v2)
}

func TestGCStateDetails(t *testing.T) {
	// In the absence of concurrency, these tests call
	// |waitForNoGC| without gcMu held.
	t.Run("StartsNoGC", func(t *testing.T) {
		vs := newTestValueStore()
		vs.waitForNoGC()
	})
	t.Run("NewGenBeforeOldGenPanics", func(t *testing.T) {
		vs := newTestValueStore()
		assert.Panics(t, func() {
			vs.transitionToNewGenGC()
		})
	})
	t.Run("FinalizingBeforeNewGenPanics", func(t *testing.T) {
		vs := newTestValueStore()
		assert.Panics(t, func() {
			vs.transitionToFinalizingGC()
		})
		vs = newTestValueStore()
		vs.transitionToOldGenGC()
		assert.Panics(t, func() {
			vs.transitionToFinalizingGC()
		})
	})
	t.Run("NoGCAlwaysPossible", func(t *testing.T) {
		vs := newTestValueStore()
		vs.transitionToNoGC()
		vs.waitForNoGC()
		vs.transitionToOldGenGC()
		vs.transitionToNoGC()
		vs.waitForNoGC()
		vs.transitionToOldGenGC()
		vs.transitionToNewGenGC()
		vs.transitionToNoGC()
		vs.waitForNoGC()
		vs.transitionToOldGenGC()
		vs.transitionToNewGenGC()
		vs.transitionToFinalizingGC()
		vs.transitionToNoGC()
		vs.waitForNoGC()
	})
	t.Run("transitionToOldGenGC_Concurrent", func(t *testing.T) {
		vs := newTestValueStore()
		var wg sync.WaitGroup
		const numThreads = 16
		wg.Add(numThreads)
		var running atomic.Int32
		for i := 0; i < numThreads; i++ {
			go func() {
				// We attempt to yield a bunch to get parallelism, but
				// in reality this test is best-effort looking for
				// wonkiness where transitionToOldGenGC allows more
				// than one thread to enter GC at a time or something
				// like that.
				defer wg.Done()
				runtime.Gosched()
				vs.transitionToOldGenGC()
				require.True(t, running.CompareAndSwap(0, 1))
				runtime.Gosched()
				vs.transitionToNewGenGC()
				runtime.Gosched()
				vs.transitionToFinalizingGC()
				runtime.Gosched()
				require.True(t, running.CompareAndSwap(1, 0))
				vs.transitionToNoGC()
			}()
		}
		wg.Wait()
	})
	t.Run("gcAddChunk", func(t *testing.T) {
		t.Run("NoGCPanics", func(t *testing.T) {
			vs := newTestValueStore()
			assert.Panics(t, func() {
				vs.gcAddChunk(hash.Hash{})
			})
		})
		t.Run("OldGenAddsChunk", func(t *testing.T) {
			vs := newTestValueStore()
			vs.transitionToOldGenGC()
			assert.False(t, vs.gcAddChunk(hash.Hash{}))
			got := vs.readAndResetNewGenToVisit()
			assert.Len(t, got, 0)
			got = vs.transitionToNewGenGC()
			assert.Len(t, got, 1)
		})
		t.Run("NewGenAddsChunk", func(t *testing.T) {
			t.Run("SeenInReadAndReset", func(t *testing.T) {
				vs := newTestValueStore()
				vs.transitionToOldGenGC()
				assert.Len(t, vs.transitionToNewGenGC(), 0)
				assert.False(t, vs.gcAddChunk(hash.Hash{}))
				assert.Len(t, vs.readAndResetNewGenToVisit(), 1)
				assert.Len(t, vs.transitionToFinalizingGC(), 0)
			})
			t.Run("SeenInTransitionToFinalizing", func(t *testing.T) {
				vs := newTestValueStore()
				vs.transitionToOldGenGC()
				assert.Len(t, vs.transitionToNewGenGC(), 0)
				assert.False(t, vs.gcAddChunk(hash.Hash{}))
				assert.Len(t, vs.transitionToFinalizingGC(), 1)
			})
		})
		t.Run("Finalizing", func(t *testing.T) {
			t.Run("WantsBlock", func(t *testing.T) {
				vs := newTestValueStore()
				vs.transitionToOldGenGC()
				vs.transitionToNewGenGC()
				vs.transitionToFinalizingGC()
				assert.True(t, vs.gcAddChunk(hash.Hash{}))
			})
			t.Run("WithOutstandingOp", func(t *testing.T) {
				vs := newTestValueStore()
				var cleanups []func()
				cleanup, err := vs.waitForNotFinalizingGC(context.Background())
				assert.NoError(t, err)
				cleanups = append(cleanups, cleanup)
				vs.transitionToOldGenGC()
				cleanup, err = vs.waitForNotFinalizingGC(context.Background())
				assert.NoError(t, err)
				cleanups = append(cleanups, cleanup)
				vs.transitionToNewGenGC()
				cleanup, err = vs.waitForNotFinalizingGC(context.Background())
				assert.NoError(t, err)
				cleanups = append(cleanups, cleanup)
				var seen hash.HashSet
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					seen = vs.transitionToFinalizingGC()
				}()
				runtime.Gosched()
				assert.False(t, vs.gcAddChunk(hash.Hash{}))
				for _, c := range cleanups {
					c()
				}
				wg.Wait()
				assert.Len(t, seen, 1)
			})
		})
	})
	t.Run("WaitForFinalizing", func(t *testing.T) {
		t.Run("CtxDone", func(t *testing.T) {
			vs := newTestValueStore()
			vs.transitionToOldGenGC()
			vs.transitionToNewGenGC()
			vs.transitionToFinalizingGC()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			cleanup, err := vs.waitForNotFinalizingGC(ctx)
			assert.Nil(t, cleanup)
			assert.Error(t, err)
		})
		t.Run("Blocking", func(t *testing.T) {
			vs := newTestValueStore()
			vs.transitionToOldGenGC()
			vs.transitionToNewGenGC()
			vs.transitionToFinalizingGC()
			ctx := context.Background()
			var cleanup func()
			var err error
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				cleanup, err = vs.waitForNotFinalizingGC(ctx)
			}()
			vs.transitionToNoGC()
			wg.Wait()
			assert.NoError(t, err)
			assert.NotNil(t, cleanup)
			cleanup()
		})
	})
}

type badVersionStore struct {
	chunks.ChunkStore
}

func (b *badVersionStore) Version() string {
	return "BAD"
}

type purgingSafepointController struct {
	vs *ValueStore
}

var _ (GCSafepointController) = purgingSafepointController{}

func (c purgingSafepointController) BeginGC(ctx context.Context, keeper func(h hash.Hash) bool) error {
	c.vs.PurgeCaches()
	return nil
}

func (c purgingSafepointController) EstablishPreFinalizeSafepoint(context.Context) error {
	return nil
}

func (c purgingSafepointController) EstablishPostFinalizeSafepoint(context.Context) error {
	return nil
}

func (c purgingSafepointController) CancelSafepoint() {
}
