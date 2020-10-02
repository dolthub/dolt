// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSizeCache(t *testing.T) {
	defSize := manifestContents{}.size()

	t.Run("GetAndPut", func(t *testing.T) {
		assert := assert.New(t)

		c := newManifestCache(2 * defSize)
		t1 := time.Now()
		dbA, contentsA := "dbA", manifestContents{lock: computeAddr([]byte("lockA"))}
		dbB, contentsB := "dbB", manifestContents{lock: computeAddr([]byte("lockB"))}

		err := c.Put(dbA, contentsA, t1)
		assert.NoError(err)
		err = c.Put(dbB, contentsB, t1)
		assert.NoError(err)

		cont, _, present := c.Get(dbA)
		assert.True(present)
		assert.Equal(contentsA, cont)

		cont, _, present = c.Get(dbB)
		assert.True(present)
		assert.Equal(contentsB, cont)
	})

	t.Run("PutDropsLRU", func(t *testing.T) {
		assert := assert.New(t)

		capacity := uint64(5)
		c := newManifestCache(capacity * defSize)
		keys := []string{"db1", "db2", "db3", "db4", "db5", "db6", "db7", "db8", "db9"}
		for i, v := range keys {
			err := c.Put(v, manifestContents{}, time.Now())
			assert.NoError(err)
			expected := uint64(i + 1)
			if expected >= capacity {
				expected = capacity
			}
			assert.Equal(expected*defSize, c.totalSize)
		}

		lru := len(keys) - int(capacity)
		for _, db := range keys[:lru] {
			_, _, present := c.Get(db)
			assert.False(present)
		}
		for _, db := range keys[lru:] {
			_, _, present := c.Get(db)
			assert.True(present)
		}

		// Bump |keys[lru]| to the back of the queue, making |keys[lru+1]| the next one to be dropped
		_, _, ok := c.Get(keys[lru])
		assert.True(ok)
		lru++
		err := c.Put("novel", manifestContents{}, time.Now())
		assert.NoError(err)
		_, _, ok = c.Get(keys[lru])
		assert.False(ok)
		// |keys[lru]| is gone, so |keys[lru+1]| is next
		lru++

		// Putting a bigger value will dump multiple existing entries
		err = c.Put("big", manifestContents{nomsVers: "big version"}, time.Now())
		assert.NoError(err)
		_, _, ok = c.Get(keys[lru])
		assert.False(ok)
		lru++
		_, _, ok = c.Get(keys[lru])
		assert.False(ok)
		lru++

		// Make sure expected stuff is still in the cache
		for i := lru; i < len(keys); i++ {
			_, _, ok := c.Get(keys[i])
			assert.True(ok)
		}
		for _, key := range []string{"novel", "big"} {
			_, _, ok := c.Get(key)
			assert.True(ok)
		}
	})

	t.Run("TooLargeValue", func(t *testing.T) {
		c := newManifestCache(16)
		err := c.Put("db", manifestContents{}, time.Now())
		assert.NoError(t, err)
		_, _, ok := c.Get("db")
		assert.False(t, ok)
	})

	t.Run("ZeroSizeCache", func(t *testing.T) {
		c := newManifestCache(0)
		err := c.Put("db", manifestContents{}, time.Now())
		assert.NoError(t, err)
		_, _, ok := c.Get("db")
		assert.False(t, ok)
	})

}
