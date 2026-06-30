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
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

// countingBlobReaderGitAPI counts BlobReader calls so a test can assert how many
// times a blob is streamed through `git cat-file`.
type countingBlobReaderGitAPI struct {
	git.GitAPI
	blobReaderCalls atomic.Int64
}

func (c *countingBlobReaderGitAPI) BlobReader(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
	c.blobReaderCalls.Add(1)
	return c.GitAPI.BlobReader(ctx, oid)
}

// TestGitBlobstore_RangedReads_MaterializeBlobOnce is the regression guard for the
// O(reads * blobsize) re-inflation pathology: serving each ranged read by
// streaming the whole blob and discarding the prefix meant N ranged reads of one
// table file streamed (and re-inflated) the entire blob N times. After
// materialization, many ranged reads of the same blob stream it through
// `git cat-file` exactly once and serve every subsequent range from the local
// file. The test asserts the call count, not wall-clock time, so it is the
// contract that prevents the regression rather than a timing heuristic.
func TestGitBlobstore_RangedReads_MaterializeBlobOnce(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	remoteRepo, localRepo, _ := newRemoteAndLocalRepos(t, ctx)

	// A blob large enough that the index/footer reads NBS does are genuine
	// ranged reads into the middle and tail of a single inline blob.
	const blobSize = 64 * 1024
	want := make([]byte, blobSize)
	for i := range want {
		want[i] = byte(i)
	}
	_, err := remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("seed\n"),
		"table":    want,
	}, "seed remote")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
	})
	require.NoError(t, err)
	defer bs.Close()

	// Prime the cache (one fetch) before swapping in the counting API so the
	// count reflects only the ranged reads under test.
	ok, err := bs.Exists(ctx, "table")
	require.NoError(t, err)
	require.True(t, ok)

	counter := &countingBlobReaderGitAPI{GitAPI: bs.api}
	bs.api = counter

	// Many small ranged reads scattered across the blob — including a tail
	// (negative-offset) read, NBS's worst case under the old prefix-discard path.
	const reads = 64
	for i := 0; i < reads; i++ {
		off := int64(i) * (blobSize / reads)
		got, _, err := GetBytes(ctx, bs, "table", NewBlobRange(off, 128))
		require.NoError(t, err)
		require.Equal(t, want[off:off+128], got)
	}
	tail, _, err := GetBytes(ctx, bs, "table", NewBlobRange(-256, 0))
	require.NoError(t, err)
	require.Equal(t, want[blobSize-256:], tail)

	require.Equal(t, int64(1), counter.blobReaderCalls.Load(),
		"expected the blob to be streamed exactly once and all ranges served from the materialized file")

	// A full read still streams directly (no materialization needed) and returns
	// the whole blob.
	full, _, err := GetBytes(ctx, bs, "table", AllRange)
	require.NoError(t, err)
	require.Equal(t, want, full)
}
