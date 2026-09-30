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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type countingOIDAPI struct {
	git.GitAPI
	readers map[git.OID]int
	sizes   int
	batches int
}

func (a *countingOIDAPI) BlobReader(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
	if a.readers == nil {
		a.readers = make(map[git.OID]int)
	}
	a.readers[oid]++
	return a.GitAPI.BlobReader(ctx, oid)
}

func (a *countingOIDAPI) BlobSize(ctx context.Context, oid git.OID) (int64, error) {
	a.sizes++
	return a.GitAPI.BlobSize(ctx, oid)
}

func (a *countingOIDAPI) BlobSizes(ctx context.Context, oids []git.OID) ([]int64, error) {
	a.batches++
	return a.GitAPI.BlobSizes(ctx, oids)
}

// Reproduce #11916 at the Get API, bypassing NBS table-file spooling.
func TestGitBlobstore_OIDCacheRepeatedRanges(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, _ := newRemoteAndLocalRepos(t, ctx)
	part1, part2 := bytes.Repeat([]byte("a"), 1024), bytes.Repeat([]byte("b"), 1024)
	_, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"large/0001": part1,
		"large/0002": part2,
		"inline":     []byte("0123456789"),
	}, "seed")
	require.NoError(t, err)
	bs, err := newTestGitBlobstore(t, local.GitDir, DoltDataRef)
	require.NoError(t, err)
	api := &countingOIDAPI{GitAPI: bs.api}
	bs.api = api
	for i := 0; i < 5; i++ {
		got, _, err := GetBytes(ctx, bs, "large", NewBlobRange(int64(10+i*100), 10))
		require.NoError(t, err)
		require.Equal(t, part1[:10], got)
	}
	require.Equal(t, 1, api.batches)
	require.Zero(t, api.sizes)
	require.Len(t, api.readers, 1)
	for _, count := range api.readers {
		require.Equal(t, 1, count)
	}
	got, _, err := GetBytes(ctx, bs, "large", NewBlobRange(1020, 8))
	require.NoError(t, err)
	require.Equal(t, []byte("aaaabbbb"), got)
	got, _, err = GetBytes(ctx, bs, "large", NewBlobRange(-10, 0))
	require.NoError(t, err)
	require.Equal(t, part2[:10], got)
	require.Equal(t, 1, api.batches)
	require.Len(t, api.readers, 2)
	for _, count := range api.readers {
		require.Equal(t, 1, count)
	}
	for i := 0; i < 5; i++ {
		got, _, err := GetBytes(ctx, bs, "inline", NewBlobRange(int64(i), 2))
		require.NoError(t, err)
		require.Equal(t, []byte("0123456789")[i:i+2], got)
	}
	require.Equal(t, 1, api.sizes)
	require.Len(t, api.readers, 3)
	for _, count := range api.readers {
		require.Equal(t, 1, count)
	}
}

func TestGitBlobstore_DiskOIDCacheCleanupAndFallback(t *testing.T) {
	requireGitOnPath(t)
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "unavailable"}[unavailable], func(t *testing.T) {
			ctx := context.Background()
			remote, local, _ := newRemoteAndLocalRepos(t, ctx)
			_, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"inline": []byte("abcdef")}, "seed")
			require.NoError(t, err)
			root := filepath.Join(filepath.Dir(local.GitDir), "oid-cache")
			if unavailable {
				require.NoError(t, os.WriteFile(root, nil, 0600))
			}
			bs, err := NewGitBlobstore(local.GitDir, DoltDataRef)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bs.Close()) })
			require.IsType(t, &DiskOIDCache{}, bs.objectCache())
			api := &countingOIDAPI{GitAPI: bs.api}
			bs.api = api
			for i := 0; i < 3; i++ {
				got, _, err := GetBytes(ctx, bs, "inline", NewBlobRange(int64(i), 2))
				require.NoError(t, err)
				require.Equal(t, []byte("abcdef")[i:i+2], got)
			}
			for _, count := range api.readers {
				if unavailable {
					require.Equal(t, 3, count)
				} else {
					require.Equal(t, 1, count)
				}
			}
			if !unavailable {
				rc, _, _, err := bs.Get(ctx, "inline", AllRange)
				require.NoError(t, err)
				require.NoError(t, bs.Teardown(ctx))
				got, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.Equal(t, "abcdef", string(got))
				require.NoError(t, rc.Close())
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Empty(t, entries)
			}
			// Close clears content but preserves the existing contract that
			// cached Git objects remain readable without another fetch.
			require.NoError(t, bs.Close())
			got, _, err := GetBytes(ctx, bs, "inline", AllRange)
			require.NoError(t, err)
			require.Equal(t, "abcdef", string(got))
		})
	}
}

func TestGitBlobstore_PutDoesNotPopulateOIDCache(t *testing.T) {
	requireGitOnPath(t)
	_, local, _ := newRemoteAndLocalRepos(t, context.Background())
	bs, err := newTestGitBlobstore(t, local.GitDir, DoltDataRef)
	require.NoError(t, err)
	_, err = PutBytes(context.Background(), bs, "key", []byte("hello"))
	require.NoError(t, err)
	obj, ok := bs.cacheGetObject("key")
	require.True(t, ok)
	_, ok = bs.objectCache().LookupSize(obj.oid.String())
	require.False(t, ok)
	api := &countingOIDAPI{GitAPI: bs.api}
	bs.api = api
	got, _, err := GetBytes(context.Background(), bs, "key", AllRange)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
	require.Equal(t, 1, api.readers[obj.oid])
}
