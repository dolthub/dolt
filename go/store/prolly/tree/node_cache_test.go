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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/hash"
)

// cacheInsert does the get/insert round trip a reader does, for tests
// which only care that the node ends up cached.
func cacheInsert(c nodeCache, addr hash.Hash, n *Node) {
	_, key, _ := c.get(addr)
	c.insert(key, n)
}

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
			cacheInsert(cache, addr, n)
		}
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			_, _, ok := cache.get(addr)
			assert.True(t, ok)
		}
		cache.purge()
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			_, _, ok := cache.get(addr)
			assert.False(t, ok)
		}
	})

	t.Run("InsertAfterPurgeIsDropped", func(t *testing.T) {
		var addr hash.Hash
		n := &Node{
			msg: make([]byte, 1024),
		}
		cache := newChunkCache(256 * 1024)

		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			// Miss, purge, then insert what the reader fetched.
			_, key, ok := cache.get(addr)
			assert.False(t, ok)
			cache.purge()
			cache.insert(key, n)

			_, _, ok = cache.get(addr)
			assert.False(t, ok)
		}

		// A key obtained after the purge still lands.
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			cacheInsert(cache, addr, n)
			_, _, ok := cache.get(addr)
			assert.True(t, ok)
		}
	})

	t.Run("PurgeOnlyDropsInsertsForItsOwnStripe", func(t *testing.T) {
		// A key is only checked against its own stripe, which is what
		// keeps nodeStore.ReadMany, holding a key per miss across one
		// GetMany, from losing every node when an unrelated stripe is
		// purged.
		n := &Node{
			msg: make([]byte, 1024),
		}
		cache := newChunkCache(256 * 1024)

		var first, second hash.Hash
		second[0] = 1

		_, key, _ := cache.get(first)
		cache[second[0]].purge()

		cache.insert(key, n)
		_, _, ok := cache.get(first)
		assert.True(t, ok)
	})
}
