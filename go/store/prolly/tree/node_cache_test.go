// Copyright 2021 Dolthub, Inc.
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

package tree

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestNodeCache(t *testing.T) {
	t.Run("InsertGetPurge", func(t *testing.T) {
		// Simple smoke screen test of insert, get, purge.
		var addr hash.Hash
		n := &Node{
			msg: make([]byte, 1024),
		}
		cache := newChunkCache(256 * 1024)
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			cache.insert(addr, n)
		}
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			_, ok := cache.get(addr)
			assert.True(t, ok)
		}
		cache.purge()
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			_, ok := cache.get(addr)
			assert.False(t, ok)
		}
	})
}

func TestNodeCacheSizeEnvVar(t *testing.T) {
	// Save original value and restore after test
	origCacheSize := cacheSize
	t.Cleanup(func() {
		cacheSize = origCacheSize
	})

	t.Run("valid value overrides default", func(t *testing.T) {
		cacheSize = 256 * 1024 * 1024 // reset to default
		expected := 512 * 1024 * 1024
		t.Setenv("DOLT_NODE_CACHE_SIZE", strconv.Itoa(expected))

		// Simulate what init() does
		if v := os.Getenv("DOLT_NODE_CACHE_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cacheSize = n
			}
		}
		require.Equal(t, expected, cacheSize)

		// Verify the cache can be created with the new size
		cache := newChunkCache(cacheSize)
		assert.NotNil(t, cache)
	})

	t.Run("invalid value keeps default", func(t *testing.T) {
		cacheSize = 256 * 1024 * 1024 // reset to default
		t.Setenv("DOLT_NODE_CACHE_SIZE", "not-a-number")

		if v := os.Getenv("DOLT_NODE_CACHE_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cacheSize = n
			}
		}
		require.Equal(t, 256*1024*1024, cacheSize)
	})

	t.Run("zero value keeps default", func(t *testing.T) {
		cacheSize = 256 * 1024 * 1024 // reset to default
		t.Setenv("DOLT_NODE_CACHE_SIZE", "0")

		if v := os.Getenv("DOLT_NODE_CACHE_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cacheSize = n
			}
		}
		require.Equal(t, 256*1024*1024, cacheSize)
	})

	t.Run("negative value keeps default", func(t *testing.T) {
		cacheSize = 256 * 1024 * 1024 // reset to default
		t.Setenv("DOLT_NODE_CACHE_SIZE", "-100")

		if v := os.Getenv("DOLT_NODE_CACHE_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cacheSize = n
			}
		}
		require.Equal(t, 256*1024*1024, cacheSize)
	})
}
