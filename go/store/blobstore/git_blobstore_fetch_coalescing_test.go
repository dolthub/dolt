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

package blobstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

// countingFetchGitAPI counts FetchRef calls and tracks how many ran at once, so
// tests can assert that a burst of cache-missing reads costs one fetch.
type countingFetchGitAPI struct {
	git.GitAPI
	total    atomic.Int64
	inFlight atomic.Int64
	maxSeen  atomic.Int64
	// delay, when non-zero, holds each fetch open long enough for the rest of a
	// concurrent burst to reach the coalescing point.
	delay time.Duration
	// started is closed when the first fetch begins.
	started     chan struct{}
	startedOnce sync.Once
	// canceled receives the cause of every fetch that ended because its own
	// context was canceled. Buffered, so a test that ignores it never blocks a
	// fetch. Its length is also the count of such fetches.
	canceled chan error
}

func (c *countingFetchGitAPI) FetchRef(ctx context.Context, remote, srcRef, dstRef string) error {
	c.total.Add(1)
	c.startedOnce.Do(func() { close(c.started) })
	n := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		max := c.maxSeen.Load()
		if n <= max || c.maxSeen.CompareAndSwap(max, n) {
			break
		}
	}
	if c.delay > 0 {
		// A real fetch dies when its context is canceled, and the point of a
		// fetch owning its context is who gets to do that, so stand in for one
		// faithfully rather than sleeping through cancellation.
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			select {
			case c.canceled <- context.Cause(ctx):
			default:
			}
			return context.Cause(ctx)
		}
	}
	return c.GitAPI.FetchRef(ctx, remote, srcRef, dstRef)
}

// awaitFetchStarted waits until |n| fetches have begun. The count rises before a
// fetch does anything, so this is the signal that the nth one is in flight.
func (c *countingFetchGitAPI) awaitFetchStarted(t *testing.T, n int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return c.total.Load() >= n
	}, 5*time.Second, time.Millisecond, "fetch %d never started", n)
}

// awaitCanceledFetch returns the cause of the next fetch to end in cancellation.
func (c *countingFetchGitAPI) awaitCanceledFetch(t *testing.T) error {
	t.Helper()
	select {
	case cause := <-c.canceled:
		return cause
	case <-time.After(5 * time.Second):
		t.Fatal("no fetch was canceled")
		return nil
	}
}

// newCountingBlobstore returns a blobstore whose fetches are counted, along with
// the counter. |ttl| is the read-side dedup window; a nanosecond effectively
// disables it.
func newCountingBlobstore(t *testing.T, ctx context.Context, tree map[string][]byte, ttl time.Duration) (*GitBlobstore, *gitrepo.Repo, *countingFetchGitAPI) {
	t.Helper()

	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, tree, "seed remote")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName:     "origin",
		SyncForReadTTL: ttl,
	})
	require.NoError(t, err)

	counting := &countingFetchGitAPI{
		GitAPI:   bs.api,
		started:  make(chan struct{}),
		canceled: make(chan error, 16),
	}
	bs.api = counting
	// Close cancels and drains any fetch still running, which keeps a fetch
	// goroutine from reading the repos t.TempDir is about to remove.
	t.Cleanup(func() { require.NoError(t, bs.Close()) })
	return bs, remoteRepo, counting
}

// A burst of concurrent readers must not each run its own fetch: one fetch
// brings the whole ref, and for an ssh remote each extra one is a separate
// connection and key exchange.
func TestGitBlobstore_ConcurrentReadsCoalesceIntoOneFetch(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	// Hold each fetch open so the whole burst reaches the coalescing point
	// while the first fetch is still running.
	counting.delay = 100 * time.Millisecond

	// The manifest is never served from the cache, so every one of these would
	// fetch if fetches were not coalesced.
	const readers = 24
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, errs[i] = GetBytes(ctx, bs, "manifest", AllRange)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, int64(1), counting.maxSeen.Load(), "fetches must not overlap")
	require.Less(t, counting.total.Load(), int64(readers),
		"expected concurrent readers to share a fetch, got %d fetches for %d readers",
		counting.total.Load(), readers)
}

// A fetch that finds the remote head unchanged still refreshes the dedup
// window. Otherwise the window never renews against a quiet remote and every
// cache-missing read fetches again forever.
func TestGitBlobstore_UnchangedHeadRenewsDedupWindow(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	const ttl = 75 * time.Millisecond
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, ttl)

	_, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, int64(1), counting.total.Load())

	// Let the window lapse. The next read fetches, and finds the same head.
	time.Sleep(ttl * 2)
	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, int64(2), counting.total.Load())

	// That fetch renewed the window, so an immediate read reuses it.
	_, _, err = GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, int64(2), counting.total.Load(), "unchanged head did not renew the dedup window")
}

// Once the cache has been merged from a commit, it holds that commit's complete
// listing, so an absent key is answered without going to the remote.
func TestGitBlobstore_AbsentKeyIsAnsweredWithoutFetching(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
		"present":  []byte("abc"),
	}, time.Nanosecond)

	// Warm the cache.
	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)
	warm := counting.total.Load()
	require.Greater(t, warm, int64(0))

	// This is the shape conjoin produces: many existence checks for records
	// sub-objects that were never written.
	for i := 0; i < 50; i++ {
		ok, err := bs.Exists(ctx, "absent.records")
		require.NoError(t, err)
		require.False(t, ok)

		_, _, _, err = bs.Get(ctx, "absent.records", AllRange)
		require.Error(t, err)
		require.True(t, IsNotFoundError(err))
	}
	require.Equal(t, warm, counting.total.Load(), "absent keys must not trigger a fetch")

	// A present key is still served, and still without fetching.
	got, _, err := GetBytes(ctx, bs, "present", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got)
	require.Equal(t, warm, counting.total.Load())
}

// The manifest is the escape hatch that keeps authoritative absence safe: it is
// never served from the cache, so the sync that delivers a new manifest also
// delivers the keys that manifest names.
func TestGitBlobstore_NewRemoteKeysArriveWithTheManifest(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, remoteRepo, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("gen1\n"),
	}, time.Nanosecond)

	// Warm the cache, then confirm the not-yet-pushed table reads as absent
	// without a fetch.
	_, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	warm := counting.total.Load()

	ok, err := bs.Exists(ctx, "table")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, warm, counting.total.Load())

	// Another writer pushes a new manifest naming a new table file.
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("gen2\n"),
		"table":    []byte("xyz"),
	}, "second writer")
	require.NoError(t, err)

	// Reading the manifest always fetches, which brings the new table with it.
	got, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("gen2\n"), got)
	require.Greater(t, counting.total.Load(), warm, "the manifest must always fetch")

	ok, err = bs.Exists(ctx, "table")
	require.NoError(t, err)
	require.True(t, ok, "a key named by a freshly fetched manifest must be visible")

	gotTable, _, err := GetBytes(ctx, bs, "table", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("xyz"), gotTable)
}

// Waiting on another reader's fetch must stay cancellable. That fetch belongs to
// a different caller, which may have no deadline of its own (conjoin runs on
// context.Background()), so a hung connection must not be able to block every
// other reader in the process.
func TestGitBlobstore_WaitingOnAnotherFetchHonorsContext(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	// Stand in for a hung connection, on a caller that will not give up. Long
	// enough that a blocked waiter is unmistakable, short enough that the test
	// can wait it out.
	const stuckFor = 2 * time.Second
	counting.delay = stuckFor

	stuck := make(chan struct{})
	go func() {
		defer close(stuck)
		_, _, _, _ = bs.Get(context.Background(), "manifest", AllRange)
	}()
	// Don't outlive the fetch: it reads the repos that t.TempDir removes.
	t.Cleanup(func() { <-stuck })
	<-counting.started

	// A second reader arrives while that fetch is stuck. It must give up on its
	// own deadline rather than inherit the stuck caller's.
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := bs.Exists(waitCtx, "manifest")
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, stuckFor/2, "waiter blocked on the stuck fetch")
	require.Equal(t, int64(1), counting.total.Load(), "the waiter must not start its own fetch")
	require.Empty(t, counting.canceled, "a waiter leaving must not end a fetch another reader needs")
}

// A fetch is shared, so it must not answer to any one reader's deadline. The
// reader that happened to start it giving up cannot be allowed to fail the
// readers waiting behind it, which still have time of their own.
func TestGitBlobstore_FetchOutlivesTheReaderThatStartedIt(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	// Longer than the first reader's deadline, shorter than the second's.
	counting.delay = 300 * time.Millisecond

	firstCtx, cancelFirst := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := GetBytes(firstCtx, bs, "manifest", AllRange)
		firstDone <- err
	}()
	<-counting.started

	// A second reader joins the fetch the first one started, then outlives it.
	secondCtx, cancelSecond := context.WithTimeout(ctx, 30*time.Second)
	defer cancelSecond()
	secondDone := make(chan error, 1)
	go func() {
		got, _, err := GetBytes(secondCtx, bs, "manifest", AllRange)
		if err == nil {
			require.Equal(t, []byte("hello\n"), got)
		}
		secondDone <- err
	}()

	require.ErrorIs(t, <-firstDone, context.DeadlineExceeded)
	require.NoError(t, <-secondDone, "the first reader's deadline must not fail a reader that still had time")
	require.Equal(t, int64(1), counting.total.Load(), "the second reader must not start its own fetch")
	require.Empty(t, counting.canceled, "the fetch had a waiter throughout and must not have been canceled")
}

// A fetch nobody is waiting for is work nobody asked for, so the last reader to
// leave ends it.
func TestGitBlobstore_LastReaderLeavingEndsTheFetch(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	// Stand in for a hung connection: far longer than the only reader will wait.
	counting.delay = 30 * time.Second

	readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := bs.Exists(readCtx, "manifest")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.ErrorIs(t, counting.awaitCanceledFetch(t), errGitSyncAbandoned)
	require.Equal(t, int64(1), counting.total.Load())
}

// The abandoned fetch's git process is still exiting when the next reader
// arrives. Only one fetch may run against the remote-tracking ref at a time, so
// that reader waits it out rather than starting a second one alongside it.
func TestGitBlobstore_NextFetchWaitsForTheAbandonedOne(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	counting.delay = 30 * time.Second

	readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := bs.Exists(readCtx, "manifest")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Arrive while the abandoned fetch is winding down. This read must still be
	// served, by a fetch of its own.
	counting.delay = 0
	got, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("hello\n"), got)

	require.Equal(t, int64(2), counting.total.Load(), "the second reader needed a fetch of its own")
	require.Equal(t, int64(1), counting.maxSeen.Load(), "fetches must not overlap")
	require.ErrorIs(t, counting.awaitCanceledFetch(t), errGitSyncAbandoned)
}

// Closing the store ends a fetch running under it, and says so. Afterwards the
// store answers from the cache it already has and never fetches again.
func TestGitBlobstore_CloseEndsAndDrainsTheFetch(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
		"present":  []byte("abc"),
	}, time.Nanosecond)

	// Warm the cache, so there is something to answer from after the close.
	_, _, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// A reader with no deadline of its own, the shape conjoin has.
	counting.delay = 30 * time.Second
	readDone := make(chan error, 1)
	go func() {
		_, _, err := GetBytes(context.Background(), bs, "manifest", AllRange)
		readDone <- err
	}()
	counting.awaitFetchStarted(t, 2)

	require.NoError(t, bs.Close())
	require.Zero(t, counting.inFlight.Load(), "Close returned with a fetch still running")
	require.ErrorIs(t, <-readDone, errGitStoreClosed, "a read caught by Close must say what happened")
	require.ErrorIs(t, counting.awaitCanceledFetch(t), errGitStoreClosed)

	// The cache outlives the close, and nothing goes to the remote for it.
	got, _, err := GetBytes(ctx, bs, "present", AllRange)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got)
	ok, err := bs.Exists(ctx, "manifest")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), counting.total.Load(), "a closed store must not fetch")
}

// Teardown deletes the remote-tracking ref a fetch writes and repacks the
// objects it reads, so it must not run alongside one.
func TestGitBlobstore_TeardownEndsAndDrainsTheFetch(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	bs, _, counting := newCountingBlobstore(t, ctx, map[string][]byte{
		"manifest": []byte("hello\n"),
	}, time.Nanosecond)
	counting.delay = 30 * time.Second

	readDone := make(chan error, 1)
	go func() {
		_, _, err := GetBytes(context.Background(), bs, "manifest", AllRange)
		readDone <- err
	}()
	<-counting.started

	require.NoError(t, bs.Teardown(ctx))
	require.Zero(t, counting.inFlight.Load(), "Teardown returned with a fetch still running")
	require.ErrorIs(t, <-readDone, errGitStoreTornDown, "a read caught by Teardown must say what happened")
	require.ErrorIs(t, counting.awaitCanceledFetch(t), errGitStoreTornDown)
}
