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

package mutexmap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// size reports how many mutexes the map is holding onto. Every mutex should be dropped once
// nobody holds or awaits it, whether or not the waiters ever acquired it.
func (mm *MutexMap) size() int {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return len(mm.keyedMutexes)
}

// lock acquires |key| with a context that is never canceled, for tests that need a holder
// rather than a waiter.
func lock(t *testing.T, mm *MutexMap, key interface{}) func() {
	t.Helper()
	release, err := mm.Lock(context.Background(), key)
	require.NoError(t, err)
	return release
}

// lockAsync acquires |key| on another goroutine, reporting the acquisition on the returned
// channel and releasing immediately. Errors come back to the test goroutine rather than
// being asserted where a failure would just hang the test.
func lockAsync(mm *MutexMap, ctx context.Context, key interface{}) <-chan error {
	acquired := make(chan error, 1)
	go func() {
		release, err := mm.Lock(ctx, key)
		if release != nil {
			release()
		}
		acquired <- err
	}()
	return acquired
}

func TestLockExcludesSameKeyOnly(t *testing.T) {
	mm := NewMutexMap()

	release := lock(t, mm, "a")

	otherKey := lockAsync(mm, context.Background(), "b")
	select {
	case err := <-otherKey:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Lock on a different key blocked behind an unrelated held lock")
	}

	sameKey := lockAsync(mm, context.Background(), "a")
	select {
	case err := <-sameKey:
		t.Fatalf("Lock on a held key returned (%v) while the lock was held", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case err := <-sameKey:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Lock did not return after the lock was released")
	}

	require.Equal(t, 0, mm.size())
}

func TestLockGivesUpOnCancellation(t *testing.T) {
	mm := NewMutexMap()

	// A long-running holder, as AcquireLock hands out for the duration of an insert
	// statement outside interleaved lock mode.
	release := lock(t, mm, "a")

	ctx, cancel := context.WithCancel(context.Background())
	blocked := lockAsync(mm, ctx, "a")

	select {
	case err := <-blocked:
		t.Fatalf("Lock returned %v while the lock was held", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-blocked:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("Lock did not return after its context was canceled")
	}

	// Giving up must not have handed the lock to the abandoned waiter, or taken it from the
	// holder: the lock is still available to whoever asks next, once released.
	release()
	lock(t, mm, "a")()

	require.Equal(t, 0, mm.size())
}

func TestLockAlreadyCanceledIsGrantedWhenUncontended(t *testing.T) {
	mm := NewMutexMap()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Nothing to wait for, so the canceled context does not deny the lock. Without the
	// non-blocking attempt this is a coin flip between the two select cases.
	for i := 0; i < 100; i++ {
		release, err := mm.Lock(ctx, "a")
		require.NoError(t, err)
		release()
	}
	require.Equal(t, 0, mm.size())
}

func TestLockAlreadyCanceledDoesNotWait(t *testing.T) {
	mm := NewMutexMap()

	release := lock(t, mm, "a")
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	acquired, err := mm.Lock(ctx, "a")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, acquired)

	// Only the holder's reference is left; the refused caller did not leak one.
	require.Equal(t, 1, mm.size())
}

func TestLockReportsCause(t *testing.T) {
	mm := NewMutexMap()
	release := lock(t, mm, "a")
	defer release()

	cause := errors.New("query killed")
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel(cause)
	}()

	_, err := mm.Lock(ctx, "a")
	require.ErrorIs(t, err, cause)
}

func TestUnlockOfUnheldMutexPanics(t *testing.T) {
	mm := NewMutexMap()
	release := lock(t, mm, "a")
	release()
	require.Panics(t, release)
}
