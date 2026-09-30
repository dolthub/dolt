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
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func testOIDCaches(t *testing.T, test func(*testing.T, OIDCache)) {
	t.Helper()
	for _, name := range []string{"memory", "disk"} {
		t.Run(name, func(t *testing.T) {
			var cache OIDCache = NewMemoryOIDCache()
			if name == "disk" {
				cache = NewDiskOIDCache(filepath.Join(t.TempDir(), "oid-cache"))
			}
			t.Cleanup(func() { require.NoError(t, cache.Clear()) })
			test(t, cache)
		})
	}
}

func TestOIDCacheConcurrentRanges(t *testing.T) {
	testOIDCaches(t, func(t *testing.T, cache OIDCache) {
		data := bytes.Repeat([]byte{0, '\n', 255, 42}, 1024)
		var loads atomic.Int32
		load := func(context.Context) (io.ReadCloser, int64, error) {
			loads.Add(1)
			return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
		}
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				obj, err := cache.GetOrLoad(context.Background(), "oid", load)
				if !assertCacheRead(t, obj, err) {
					return
				}
				defer obj.Close()
				got, err := io.ReadAll(io.NewSectionReader(obj, int64(i), 31))
				if err != nil || !bytes.Equal(got, data[i:i+31]) {
					t.Errorf("range %d: %x, %v", i, got, err)
				}
			}(i)
		}
		wg.Wait()
		require.Equal(t, int32(1), loads.Load())
		size, ok := cache.LookupSize("oid")
		require.True(t, ok)
		require.Equal(t, int64(len(data)), size)
	})
}

func assertCacheRead(t *testing.T, obj CachedObject, err error) bool {
	t.Helper()
	if err != nil || obj == nil {
		t.Errorf("cache read: %v", err)
		return false
	}
	return true
}

func TestOIDCacheClearKeepsReadersAndAllowsReload(t *testing.T) {
	testOIDCaches(t, func(t *testing.T, cache OIDCache) {
		loads := 0
		load := func(context.Context) (io.ReadCloser, int64, error) {
			loads++
			return io.NopCloser(bytes.NewBufferString("abcdef")), 6, nil
		}
		obj, err := cache.GetOrLoad(context.Background(), "oid", load)
		require.NoError(t, err)
		require.NoError(t, cache.Clear())
		_, ok := cache.LookupSize("oid")
		require.False(t, ok)
		got, err := io.ReadAll(io.NewSectionReader(obj, 1, 3))
		require.NoError(t, err)
		require.Equal(t, "bcd", string(got))
		require.NoError(t, obj.Close())
		require.NoError(t, obj.Close())
		obj, err = cache.GetOrLoad(context.Background(), "oid", load)
		require.NoError(t, err)
		require.Equal(t, 2, loads)
		require.NoError(t, obj.Close())
	})
}

func TestOIDCacheFailedLoadsAreRetried(t *testing.T) {
	testOIDCaches(t, func(t *testing.T, cache OIDCache) {
		for _, tc := range []struct {
			data string
			size int64
		}{{"short", 10}, {"too long", 1}} {
			_, err := cache.GetOrLoad(context.Background(), "oid", func(context.Context) (io.ReadCloser, int64, error) {
				return io.NopCloser(bytes.NewBufferString(tc.data)), tc.size, nil
			})
			require.Error(t, err)
			if tc.size == 10 {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
			_, ok := cache.LookupSize("oid")
			require.False(t, ok)
		}
		obj, err := cache.GetOrLoad(context.Background(), "oid", func(context.Context) (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewBufferString("ok")), 2, nil
		})
		require.NoError(t, err)
		require.NoError(t, obj.Close())
	})
}

func TestOIDCacheCanceledWaiterDoesNotCancelLoad(t *testing.T) {
	testOIDCaches(t, func(t *testing.T, cache OIDCache) {
		started, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			obj, err := cache.GetOrLoad(context.Background(), "oid", func(context.Context) (io.ReadCloser, int64, error) {
				close(started)
				<-finish
				return io.NopCloser(bytes.NewBufferString("ok")), 2, nil
			})
			if err == nil {
				err = obj.Close()
			}
			done <- err
		}()
		<-started
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := cache.GetOrLoad(ctx, "oid", nil)
		require.ErrorIs(t, err, context.Canceled)
		close(finish)
		require.NoError(t, <-done)
		obj, err := cache.GetOrLoad(context.Background(), "oid", nil)
		require.NoError(t, err)
		require.NoError(t, obj.Close())
	})
}

func TestDiskOIDCacheOwnsAndRemovesItsDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "oid-cache")
	a, b := NewDiskOIDCache(root), NewDiskOIDCache(root)
	load := func(context.Context) (io.ReadCloser, int64, error) {
		return io.NopCloser(bytes.NewBufferString("ok")), 2, nil
	}
	_, err := os.Stat(root)
	require.True(t, errors.Is(err, os.ErrNotExist))
	objA, err := a.GetOrLoad(context.Background(), "oid", load)
	require.NoError(t, err)
	objB, err := b.GetOrLoad(context.Background(), "oid", load)
	require.NoError(t, err)
	require.NoError(t, b.Clear())
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 2, "outstanding readers pin their cache directories")
	require.NoError(t, objB.Close())
	entries, err = os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, objA.Close())
	require.NoError(t, a.Clear())
	entries, err = os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestOIDCacheClearDuringLoad(t *testing.T) {
	testOIDCaches(t, func(t *testing.T, cache OIDCache) {
		started, finish := make(chan struct{}), make(chan struct{})
		result := make(chan CachedObject, 1)
		errResult := make(chan error, 1)
		go func() {
			obj, err := cache.GetOrLoad(context.Background(), "oid", func(context.Context) (io.ReadCloser, int64, error) {
				close(started)
				<-finish
				return io.NopCloser(bytes.NewBufferString("old")), 3, nil
			})
			result <- obj
			errResult <- err
		}()
		<-started
		require.NoError(t, cache.Clear())
		close(finish)
		obj := <-result
		require.NoError(t, <-errResult)
		_, ok := cache.LookupSize("oid")
		require.False(t, ok, "a retired load must not populate the new generation")
		got, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
		require.NoError(t, err)
		require.Equal(t, "old", string(got))
		require.NoError(t, obj.Close())
		if disk, ok := cache.(*DiskOIDCache); ok {
			entries, err := os.ReadDir(disk.root)
			require.NoError(t, err)
			require.Empty(t, entries)
		}
	})
}
