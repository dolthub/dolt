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

package nbs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// BeginGC must not install its keeper while a read which sampled the
// previous one is still running: that read would return chunks with no
// read dependency taken on them.
func TestBeginGCDrainsOutstandingReads(t *testing.T) {
	ctx := context.Background()

	_, _, _, st := makeStoreWithFakes(t)
	defer st.Close()

	c := chunks.NewChunk([]byte("drain-outstanding-reads"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	_, err := st.Commit(ctx, hash.Hash{}, hash.Hash{})
	require.NoError(t, err)

	// Start a read, as an unlocked read path does.
	st.mu.Lock()
	keeper, endRead, _ := st.beginRead()
	st.mu.Unlock()
	require.Nil(t, keeper, "expected no keeper before a GC has begun")

	var keeperCalls atomic.Int64
	gcKeeper := func(hash.Hash) bool {
		keeperCalls.Add(1)
		return false
	}
	begun := make(chan error, 1)
	go func() {
		begun <- st.BeginGC(ctx, gcKeeper, chunks.GCMode_Full)
	}()

	// BeginGC must not install while our read is still running.
	select {
	case err := <-begun:
		t.Fatalf("BeginGC installed its keeper while a read was outstanding: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// A new read has to queue behind the pending install rather than
	// run under the keeper it is replacing.
	got := make(chan error, 1)
	go func() {
		_, err := st.Get(ctx, c.Hash())
		got <- err
	}()

	select {
	case err := <-got:
		t.Fatalf("a read ran while BeginGC was waiting to install its keeper: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Finishing the outstanding read releases the drain.
	st.mu.Lock()
	endRead()
	st.mu.Unlock()

	select {
	case err := <-begun:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("BeginGC did not install its keeper after the outstanding read finished")
	}
	defer st.EndGC(chunks.GCMode_Full)

	// The queued read runs under the new keeper, so the chunk it hands
	// back is a GC dependency.
	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("read queued behind the install never ran")
	}
	assert.Positive(t, keeperCalls.Load(), "queued read did not consult the GC's keeper")
}

// A read which spans the memtable and the table files must sequence
// wholly on one side of a keeper installation. The memtable half is
// served with |nbs.mu| held and registers no unlocked read, so a
// barrier which parked in |beginRead| would return it under the old
// keeper and the rest under the new one.
func TestBeginGCInstallBarrierDoesNotSplitAReadInTwo(t *testing.T) {
	ctx := context.Background()

	_, _, _, st := makeStoreWithFakes(t)
	defer st.Close()

	// |inTable| is committed, so it is read through the unlocked path.
	// |inMemtable| is not, so it is served with |nbs.mu| held.
	inTable := chunks.NewChunk([]byte("split-read-in-table"))
	require.NoError(t, st.Put(ctx, inTable, noopGetAddrs))
	_, err := st.Commit(ctx, hash.Hash{}, hash.Hash{})
	require.NoError(t, err)
	inMemtable := chunks.NewChunk([]byte("split-read-in-memtable"))
	require.NoError(t, st.Put(ctx, inMemtable, noopGetAddrs))

	st.mu.Lock()
	require.NotNil(t, st.memtable, "expected a chunk to still be in the memtable")
	// Hold a read open so that the install has to wait for a drain.
	_, endRead, _ := st.beginRead()
	st.mu.Unlock()

	var mu sync.Mutex
	seen := make(map[hash.Hash]bool)
	gcKeeper := func(h hash.Hash) bool {
		mu.Lock()
		defer mu.Unlock()
		seen[h] = true
		return false
	}
	begun := make(chan error, 1)
	go func() {
		begun <- st.BeginGC(ctx, gcKeeper, chunks.GCMode_Full)
	}()

	select {
	case err := <-begun:
		t.Fatalf("BeginGC installed its keeper while a read was outstanding: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	hashes := hash.NewHashSet(inTable.Hash(), inMemtable.Hash())
	var found atomic.Int64
	got := make(chan error, 1)
	go func() {
		got <- st.GetMany(ctx, hashes, func(context.Context, *chunks.Chunk) {
			found.Add(1)
		})
	}()

	select {
	case err := <-got:
		t.Fatalf("a read ran while BeginGC was waiting to install its keeper: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	st.mu.Lock()
	endRead()
	st.mu.Unlock()

	select {
	case err := <-begun:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("BeginGC did not install its keeper after the outstanding read finished")
	}
	defer st.EndGC(chunks.GCMode_Full)

	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("read queued behind the install never ran")
	}
	assert.Equal(t, int64(2), found.Load())

	// Both halves must have gone through the new keeper.
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, seen[inTable.Hash()], "the table file half of the read did not consult the new keeper")
	assert.True(t, seen[inMemtable.Hash()], "the memtable half of the read did not consult the new keeper")
}

// A drain which cannot finish must give up with its context rather than
// block the caller forever, and must leave the store usable.
func TestBeginGCDrainRespectsContext(t *testing.T) {
	ctx := context.Background()

	_, _, _, st := makeStoreWithFakes(t)
	defer st.Close()

	c := chunks.NewChunk([]byte("drain-respects-context"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	_, err := st.Commit(ctx, hash.Hash{}, hash.Hash{})
	require.NoError(t, err)

	st.mu.Lock()
	_, endRead, _ := st.beginRead()
	st.mu.Unlock()

	cancelCtx, cancel := context.WithCancel(ctx)
	begun := make(chan error, 1)
	go func() {
		begun <- st.BeginGC(cancelCtx, func(hash.Hash) bool { return false }, chunks.GCMode_Full)
	}()

	select {
	case err := <-begun:
		t.Fatalf("BeginGC installed its keeper while a read was outstanding: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-begun:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("BeginGC did not return after its context was canceled")
	}

	st.mu.Lock()
	endRead()
	st.mu.Unlock()

	// A failed install must leave no reads parked behind a barrier
	// which is never coming down.
	_, err = st.Get(ctx, c.Hash())
	require.NoError(t, err)

	assert.False(t, st.conjoinDynamicallyDisabled(), "a failed install left conjoin disabled")

	// And a later GC can still install.
	require.NoError(t, st.BeginGC(ctx, func(hash.Hash) bool { return false }, chunks.GCMode_Full))
	st.EndGC(chunks.GCMode_Full)
}
