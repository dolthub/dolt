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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestNodeCache(t *testing.T) {
	t.Run("InsertGetPurge", func(t *testing.T) {
		// Simple smoke screen test of insert, get, purge.
		var addr hash.Hash
		var n Node
		n.msg = make([]byte, 1024)
		cache := newChunkCache(64 * 1024)
		for i := 0; i < 32; i++ {
			addr[0] = byte(i)
			cache.insert(addr, n)
		}
		for i := 0; i < 32; i++ {
			addr[0] = byte(i)
			_, ok := cache.get(addr)
			assert.True(t, ok)
		}
		cache.purge()
		for i := 0; i < 32; i++ {
			addr[0] = byte(i)
			_, ok := cache.get(addr)
			assert.False(t, ok)
		}
	})
}
