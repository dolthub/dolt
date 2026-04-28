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

package datas

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// Verify SimpleSafepointController satisfies the interface.
var _ types.GCSafepointController = (*SimpleSafepointController)(nil)

func TestSimpleSafepointController_BasicLifecycle(t *testing.T) {
	spc := &SimpleSafepointController{}
	ctx := context.Background()

	var kept []hash.Hash
	keeper := func(h hash.Hash) bool {
		kept = append(kept, h)
		return false
	}

	// BeginGC registers the keeper.
	require.NoError(t, spc.BeginGC(ctx, keeper))

	// KeepChunk should forward to the keeper.
	h := hash.Of([]byte("test-chunk"))
	spc.KeepChunk(h)
	assert.Len(t, kept, 1)
	assert.Equal(t, h, kept[0])

	// Safepoint lifecycle.
	require.NoError(t, spc.EstablishPreFinalizeSafepoint(ctx))
	require.NoError(t, spc.EstablishPostFinalizeSafepoint(ctx))
}

func TestSimpleSafepointController_WriteLockBlocksDuringSafepoint(t *testing.T) {
	spc := &SimpleSafepointController{}
	ctx := context.Background()

	require.NoError(t, spc.BeginGC(ctx, func(h hash.Hash) bool { return false }))

	// Acquire safepoint (write lock).
	require.NoError(t, spc.EstablishPreFinalizeSafepoint(ctx))

	// WriteLock should block while safepoint is held.
	var writerStarted, writerFinished atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		writerStarted.Store(true)
		unlock := spc.WriteLock()
		writerFinished.Store(true)
		unlock()
	}()

	// Give the goroutine time to start and block.
	for !writerStarted.Load() {
	}
	// Writer should be blocked (not finished).
	assert.False(t, writerFinished.Load(), "writer should be blocked during safepoint")

	// Release safepoint — writer should unblock.
	require.NoError(t, spc.EstablishPostFinalizeSafepoint(ctx))
	wg.Wait()
	assert.True(t, writerFinished.Load(), "writer should have completed after safepoint release")
}

func TestSimpleSafepointController_KeepChunkNoopWithoutGC(t *testing.T) {
	spc := &SimpleSafepointController{}

	// KeepChunk before BeginGC should be a no-op (no panic).
	h := hash.Of([]byte("test"))
	spc.KeepChunk(h) // Should not panic.
}

func TestSimpleSafepointController_ConcurrentWriters(t *testing.T) {
	spc := &SimpleSafepointController{}
	ctx := context.Background()

	var keepCount atomic.Int64
	require.NoError(t, spc.BeginGC(ctx, func(h hash.Hash) bool {
		keepCount.Add(1)
		return false
	}))

	// Multiple concurrent writers should all be able to hold read locks.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			unlock := spc.WriteLock()
			defer unlock()
			spc.KeepChunk(hash.Of([]byte{byte(id)}))
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(10), keepCount.Load(), "all 10 writers should have kept a chunk")
}
