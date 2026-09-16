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
			cache.insert(addr, n, cache.generation(addr))
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
		// A node read from the ChunkStore before a purge must not be
		// cached after it.
		var addr hash.Hash
		n := &Node{
			msg: make([]byte, 1024),
		}
		cache := newChunkCache(256 * 1024)

		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			// Read the generation, as a reader does on a miss, then
			// purge before getting around to the insert.
			gen := cache.generation(addr)
			cache.purge()
			cache.insert(addr, n, gen)

			_, _, ok := cache.get(addr)
			assert.False(t, ok)
		}

		// An insert which reads the generation after the purge still
		// lands.
		for i := 0; i < numStripes; i++ {
			addr[0] = uint8(i)
			cache.insert(addr, n, cache.generation(addr))
			_, _, ok := cache.get(addr)
			assert.True(t, ok)
		}
	})

	t.Run("GenerationFromGetMiss", func(t *testing.T) {
		// The generation handed back by a get miss is the one insert
		// expects, so that the miss/read/insert sequence in
		// nodeStore.Read needs no extra lock round trip.
		var addr hash.Hash
		n := &Node{
			msg: make([]byte, 1024),
		}
		cache := newChunkCache(256 * 1024)

		_, gen, ok := cache.get(addr)
		assert.False(t, ok)

		cache.insert(addr, n, gen)
		_, _, ok = cache.get(addr)
		assert.True(t, ok)
	})
}
