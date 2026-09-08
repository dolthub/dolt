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
}

func (c *countingFetchGitAPI) FetchRef(ctx context.Context, remote, srcRef, dstRef string) error {
	c.total.Add(1)
	c.startedOnce.Do(func() { close(c.started) })
	n := c.inFlight.Add(1)
	for {
		max := c.maxSeen.Load()
		if n <= max || c.maxSeen.CompareAndSwap(max, n) {
			break
		}
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.inFlight.Add(-1)
	return c.GitAPI.FetchRef(ctx, remote, srcRef, dstRef)
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

	counting := &countingFetchGitAPI{GitAPI: bs.api, started: make(chan struct{})}
	bs.api = counting
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
}
