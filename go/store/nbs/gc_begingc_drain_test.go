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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// BeginGC must not install its keeper while a read which sampled the
// previous one is still running. Such a read would finish having taken
// no read dependency on the chunks it returned, leaving the GC free to
// collect chunks the application is still holding.
func TestBeginGCDrainsOutstandingReads(t *testing.T) {
	ctx := context.Background()

	_, _, _, st := makeStoreWithFakes(t)
	defer st.Close()

	c := chunks.NewChunk([]byte("drain-outstanding-reads"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	_, err := st.Commit(ctx, hash.Hash{}, hash.Hash{})
	require.NoError(t, err)

	// Start a read, as an unlocked read path does. With no GC running,
	// it samples a nil keeper.
	st.mu.Lock()
	keeper, endRead, _, err := st.beginRead(ctx)
	require.NoError(t, err)
	require.Nil(t, keeper, "expected no keeper before a GC has begun")
	st.mu.Unlock()

	begun := make(chan error, 1)
	go func() {
		begun <- st.BeginGC(ctx, func(hash.Hash) bool { return false }, chunks.GCMode_Full)
	}()

	// BeginGC must not install while our read is still running.
	select {
	case err := <-begun:
		t.Fatalf("BeginGC installed its keeper while a read was outstanding: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// While BeginGC is waiting, a new read has to queue behind it rather
	// than start with the old keeper.
	newRead := make(chan keeperF, 1)
	go func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		keeper, endRead, _, err := st.beginRead(ctx)
		if err != nil {
			newRead <- nil
			return
		}
		endRead()
		newRead <- keeper
	}()

	select {
	case <-newRead:
		t.Fatal("a new read started while BeginGC was waiting to install its keeper")
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

	// The read which queued behind the install sees the new keeper.
	select {
	case keeper := <-newRead:
		assert.NotNil(t, keeper, "queued read did not pick up the GC's keeper")
	case <-time.After(10 * time.Second):
		t.Fatal("read queued behind the install never ran")
	}
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
	_, endRead, _, err := st.beginRead(ctx)
	require.NoError(t, err)
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

	// A failed install must leave no reads queued behind a barrier which
	// is never coming down, and must not leave conjoin disabled.
	st.mu.Lock()
	_, endRead2, _, err := st.beginRead(ctx)
	st.mu.Unlock()
	require.NoError(t, err)
	st.mu.Lock()
	endRead2()
	st.mu.Unlock()

	assert.False(t, st.conjoinDynamicallyDisabled(), "a failed install left conjoin disabled")

	// And a later GC can still install.
	require.NoError(t, st.BeginGC(ctx, func(hash.Hash) bool { return false }, chunks.GCMode_Full))
	st.EndGC(chunks.GCMode_Full)
}
