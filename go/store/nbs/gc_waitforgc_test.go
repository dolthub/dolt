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

// TestWaitForGCNotTrappedAcrossCycles is a regression test for a deadlock
// that previously existed in waitForGC. The bug: a goroutine blocked in
// waitForGC during GC cycle N could be trapped by GC cycle N+1 starting
// before the goroutine could observe gcInProgress=false.
//
// waitForGC used to loop on nbs.gcInProgress without distinguishing
// between GC cycles. If BeginGC for cycle N+1 set gcInProgress=true
// before the goroutine woken by cycle N's EndGC could reacquire nbs.mu
// and check the flag, the goroutine went back to sleep — stuck waiting
// for cycle N+1 to complete. But cycle N+1's safepoint controller could
// be waiting for this very goroutine's session to finish its command,
// causing a deadlock.
//
// The fix was to add a gcCycleCounter, incremented on each
// BeginGC, and passed into waitForGC. waitForGC now breaks out of its
// loop when the cycle counter changes, allowing the caller to re-evaluate
// the new cycle's keeper.
//
// The test forces this race deterministically by holding nbs.mu while
// transitioning from cycle 1 (gcInProgress=false) to cycle 2
// (gcInProgress=true), then unlocking. The blocked goroutine wakes up
// and sees gcInProgress=true from cycle 2, but gcCycleCounter has changed,
// so it breaks out of waitForGC and retries.
func TestWaitForGCNotTrappedAcrossCycles(t *testing.T) {
	ctx := context.Background()

	// Set up a store with a committed chunk.
	_, _, _, st := makeStoreWithFakes(t)
	defer st.Close()

	c := chunks.NewChunk([]byte("trapped-across-cycles"))
	err := st.Put(ctx, c, noopGetAddrs)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)

	// --- GC cycle 1 -------------------------------------------------------
	// Use a keeper that always returns true. A Put of a chunk whose hash
	// is already in the store will hit the keeperFunc check at the end of
	// addChunk (the chunkExists case) and enter waitForGC.
	keeperCalled := make(chan struct{}, 1)
	cycle1Keeper := func(h hash.Hash) bool {
		select {
		case keeperCalled <- struct{}{}:
		default:
		}
		return true
	}

	require.NoError(t, st.BeginGC(cycle1Keeper, chunks.GCMode_Full))

	// Launch a goroutine that Puts the same chunk again. addChunk holds
	// nbs.mu for the entire call. The chunk already exists in the
	// memtable, so addChunkRes == chunkExists. The keeperFunc check
	// returns true, so addChunk calls waitForGC → gcCond.Wait(), which
	// releases nbs.mu.
	putDone := make(chan error, 1)
	go func() {
		putDone <- st.Put(ctx, c, noopGetAddrs)
	}()

	// Wait until the keeper has been called. Since addChunk holds nbs.mu
	// the entire time, the goroutine still holds nbs.mu at this point.
	<-keeperCalled

	// Lock nbs.mu. This blocks until the goroutine enters
	// gcCond.Wait(), which releases nbs.mu. Once we acquire it, the
	// goroutine is definitely parked.
	st.mu.Lock()

	// --- Simulate the cross-cycle race ------------------------------------
	// While we hold nbs.mu, atomically end cycle 1 and begin cycle 2.
	// This is exactly what happens when BeginGC wins the race for nbs.mu
	// after EndGC's broadcast. Before the fix, the goroutine would wake,
	// see gcInProgress=true from cycle 2, and go back to sleep — trapped.
	// With the gcCycleCounter fix, it detects the cycle change and breaks
	// out.
	st.lockedEndGC()
	require.NoError(t, st.lockedBeginGC(func(hash.Hash) bool { return false }))
	st.mu.Unlock()

	// --- Assert: the goroutine is NOT trapped (regression check) ----------
	// The goroutine notices the gcCycleCounter changed, breaks out of
	// waitForGC, and re-evaluates the new cycle's keeper (which returns
	// false), allowing the Put to complete.
	select {
	case err := <-putDone:
		require.NoError(t, err)
		// Verify the chunk is still readable.
		got, err := st.Get(ctx, c.Hash())
		require.NoError(t, err)
		assert.False(t, got.IsEmpty())
	case <-time.After(3 * time.Second):
		// Goroutine is trapped — the cross-cycle deadlock has regressed.
		// Clean up so the goroutine can exit and the test doesn't leak.
		st.EndGC(chunks.GCMode_Full)
		<-putDone
		t.Fatal("goroutine trapped in waitForGC across GC cycles — " +
			"waitForGC does not distinguish between GC generations")
	}
}
