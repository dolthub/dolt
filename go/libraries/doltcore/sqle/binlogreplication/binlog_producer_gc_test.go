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

package binlogreplication

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// TestBinlogProducerImplementsGCPausableListener verifies the compile-time interface check.
func TestBinlogProducerImplementsGCPausableListener(t *testing.T) {
	var _ doltdb.GCPausableListener = (*binlogProducer)(nil)
}

// TestBinlogProducer_StopWithNoInflight tests that Stop() returns a channel that closes
// immediately when there are no in-flight WorkingRootUpdated operations.
func TestBinlogProducer_StopWithNoInflight(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	doneCh := bp.Stop()
	select {
	case <-doneCh:
		// Channel closed — Stop() completed successfully
	case <-time.After(time.Second):
		t.Fatal("Stop() channel did not close when no in-flight operations exist")
	}

	// While stopped, new read locks (simulating WorkingRootUpdated entry) should block
	blocked := make(chan struct{})
	go func() {
		bp.gcMu.RLock()
		close(blocked)
		bp.gcMu.RUnlock()
	}()

	select {
	case <-blocked:
		t.Fatal("RLock should be blocked while GC write lock is held")
	case <-time.After(100 * time.Millisecond):
		// Expected: blocked because Stop() holds write lock
	}

	// Resume should unblock the pending reader
	bp.Resume()
	select {
	case <-blocked:
		// Good — reader unblocked after Resume
	case <-time.After(time.Second):
		t.Fatal("RLock should be unblocked after Resume()")
	}
}

// TestBinlogProducer_StopWaitsForInflight tests that Stop() waits for in-flight
// Prolly tree traversals (simulated by holding a read lock) to complete before
// the returned channel closes.
func TestBinlogProducer_StopWaitsForInflight(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	// Simulate an in-flight WorkingRootUpdated holding the read lock
	bp.gcMu.RLock()

	doneCh := bp.Stop()

	// The channel should NOT close yet because the in-flight operation holds the read lock
	select {
	case <-doneCh:
		t.Fatal("Stop() channel should not close while in-flight operation holds read lock")
	case <-time.After(100 * time.Millisecond):
		// Expected: Stop is waiting for in-flight to complete
	}

	// Release the in-flight read lock (simulating WorkingRootUpdated returning)
	bp.gcMu.RUnlock()

	// Now the Stop() channel should close
	select {
	case <-doneCh:
		// Good — Stop() completed after in-flight drained
	case <-time.After(time.Second):
		t.Fatal("Stop() channel should close after in-flight operation releases read lock")
	}

	bp.Resume()
}

// TestBinlogProducer_MultipleInflightDrain tests that Stop() waits for multiple
// concurrent in-flight operations to drain before completing.
func TestBinlogProducer_MultipleInflightDrain(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	// Simulate 3 concurrent in-flight WorkingRootUpdated calls
	bp.gcMu.RLock()
	bp.gcMu.RLock()
	bp.gcMu.RLock()

	doneCh := bp.Stop()

	// Should not close yet
	select {
	case <-doneCh:
		t.Fatal("Stop() should not complete with 3 in-flight operations")
	case <-time.After(50 * time.Millisecond):
	}

	// Release two — still one holding
	bp.gcMu.RUnlock()
	bp.gcMu.RUnlock()

	select {
	case <-doneCh:
		t.Fatal("Stop() should not complete with 1 remaining in-flight operation")
	case <-time.After(50 * time.Millisecond):
	}

	// Release the last one
	bp.gcMu.RUnlock()

	select {
	case <-doneCh:
		// Good
	case <-time.After(time.Second):
		t.Fatal("Stop() should complete after all in-flight operations drain")
	}

	bp.Resume()
}

// TestBinlogProducer_StopResumeIdempotent tests that Stop/Resume can be called
// multiple times in sequence (as would happen with multiple GC cycles).
func TestBinlogProducer_StopResumeIdempotent(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	for i := 0; i < 3; i++ {
		doneCh := bp.Stop()
		select {
		case <-doneCh:
		case <-time.After(time.Second):
			t.Fatalf("Stop() cycle %d did not complete", i)
		}
		bp.Resume()
	}

	// After multiple cycles, read lock should still work normally
	bp.gcMu.RLock()
	bp.gcMu.RUnlock()
}

// TestBinlogProducer_ConcurrentStopAndWorkingRootUpdated tests the race between
// Stop() being called and a new WorkingRootUpdated attempting to enter.
func TestBinlogProducer_ConcurrentStopAndWorkingRootUpdated(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	// Run multiple goroutines trying to acquire read locks while stop/resume cycles
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Readers simulating WorkingRootUpdated calls
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					bp.gcMu.RLock()
					// Simulate brief work
					time.Sleep(time.Microsecond)
					bp.gcMu.RUnlock()
				}
			}
		}()
	}

	// Run a few GC stop/resume cycles concurrently with readers
	for i := 0; i < 5; i++ {
		doneCh := bp.Stop()
		<-doneCh
		// GC is running — all readers blocked
		time.Sleep(time.Millisecond)
		bp.Resume()
		time.Sleep(time.Millisecond)
	}

	close(done)
	wg.Wait()

	// No panics or deadlocks — test passes by completing
	assert.True(t, true)
}

// TestDatabaseUpdateListeners_GCPausable verifies that the GCPausableListener
// type assertion works correctly against DatabaseUpdateListeners.
func TestDatabaseUpdateListeners_GCPausable(t *testing.T) {
	bp := &binlogProducer{mu: &sync.Mutex{}}

	// Register and check type assertion
	var listener doltdb.DatabaseUpdateListener = bp
	pausable, ok := listener.(doltdb.GCPausableListener)
	require.True(t, ok, "binlogProducer should implement GCPausableListener")

	doneCh := pausable.Stop()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("Stop() via interface did not complete")
	}
	pausable.Resume()
}
