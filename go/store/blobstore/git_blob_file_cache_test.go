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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

const testBlobOID = git.OID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

// countingBlobAPI serves one blob's content and counts BlobReader calls, which
// correspond one-to-one with `git cat-file blob` subprocess spawns in the real
// implementation.
func countingBlobAPI(content []byte, reads *atomic.Int64) fakeGitAPI {
	return fakeGitAPI{
		blobReader: func(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
			reads.Add(1)
			return io.NopCloser(bytes.NewReader(content)), nil
		},
		blobSize: func(ctx context.Context, oid git.OID) (int64, error) {
			return int64(len(content)), nil
		},
	}
}

func TestBlobFileCacheMaterializesOnce(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 8192) // 128 KiB
	var reads atomic.Int64
	api := countingBlobAPI(content, &reads)
	c := newBlobFileCache(t.TempDir())
	require.NotNil(t, c)

	for i := 0; i < 5; i++ {
		f, sz, err := c.open(context.Background(), api, testBlobOID)
		require.NoError(t, err)
		require.Equal(t, int64(len(content)), sz)
		got, err := io.ReadAll(f)
		require.NoError(t, err)
		require.True(t, bytes.Equal(content, got))
		require.NoError(t, f.Close())
	}
	require.Equal(t, int64(1), reads.Load(), "blob should be materialized by a single BlobReader call")

	sz, ok := c.sizeOf(testBlobOID)
	require.True(t, ok)
	require.Equal(t, int64(len(content)), sz)
}

func TestBlobFileCacheConcurrentOpen(t *testing.T) {
	content := bytes.Repeat([]byte("z"), 1<<20)
	var reads atomic.Int64
	api := countingBlobAPI(content, &reads)
	c := newBlobFileCache(t.TempDir())
	require.NotNil(t, c)

	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, sz, err := c.open(context.Background(), api, testBlobOID)
			if err != nil {
				errs[i] = err
				return
			}
			defer f.Close()
			if sz != int64(len(content)) {
				errs[i] = io.ErrUnexpectedEOF
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), reads.Load(), "concurrent opens should share one materialization")
}

func TestBlobFileCacheDisabledByEnv(t *testing.T) {
	t.Setenv(blobFileCacheEnvDisable, "0")
	require.Nil(t, newBlobFileCache(t.TempDir()))

	t.Setenv(blobFileCacheEnvDisable, "false")
	require.Nil(t, newBlobFileCache(t.TempDir()))

	t.Setenv(blobFileCacheEnvDisable, "1")
	require.NotNil(t, newBlobFileCache(t.TempDir()))
}

func TestSliceFileBlobRanges(t *testing.T) {
	content := []byte("0123456789")
	newFile := func(t *testing.T) *os.File {
		p := filepath.Join(t.TempDir(), "blob")
		require.NoError(t, os.WriteFile(p, content, 0644))
		f, err := os.Open(p)
		require.NoError(t, err)
		return f
	}
	sz := int64(len(content))

	cases := []struct {
		name string
		br   BlobRange
		want string
	}{
		{"all", AllRange, "0123456789"},
		{"offset", NewBlobRange(4, 0), "456789"},
		{"offsetLength", NewBlobRange(2, 3), "234"},
		{"suffix", NewBlobRange(-3, 0), "789"},
		{"clamped", NewBlobRange(8, 100), "89"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc, gotSz, ver, err := sliceFileBlob(newFile(t), sz, tc.br, "v1")
			require.NoError(t, err)
			require.Equal(t, uint64(sz), gotSz)
			require.Equal(t, "v1", ver)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
			require.NoError(t, rc.Close())
		})
	}

	t.Run("invalidOffset", func(t *testing.T) {
		_, _, _, err := sliceFileBlob(newFile(t), sz, NewBlobRange(11, 0), "v1")
		require.Error(t, err)
	})
}

func TestBlobFileCacheTrim(t *testing.T) {
	dir := t.TempDir()
	c := &blobFileCache{dir: dir, maxBytes: 100, inFlight: make(map[string]chan struct{})}

	write := func(name string, size int) string {
		p := filepath.Join(dir, name[:2], name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, bytes.Repeat([]byte("x"), size), 0644))
		return p
	}
	old := write("aa11", 60)
	// Ensure distinct mtimes so LRU order is deterministic.
	require.NoError(t, os.Chtimes(old, time.Unix(1000, 0), time.Unix(1000, 0)))
	newer := write("bb22", 60)

	c.trim(60)

	_, errOld := os.Stat(old)
	require.True(t, os.IsNotExist(errOld), "least-recently-modified entry should be evicted")
	_, errNew := os.Stat(newer)
	require.NoError(t, errNew, "newest entry should survive")
}
