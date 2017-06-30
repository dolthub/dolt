// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"
	"time"

	"github.com/attic-labs/testify/assert"
)

func TestSizeCache(t *testing.T) {
	defSize := manifestContents{}.size()

	t.Run("GetAndPut", func(t *testing.T) {
		assert := assert.New(t)

		c := newManifestCache(2 * defSize)
		t1 := time.Now()
		dbA, contentsA := "dbA", manifestContents{lock: computeAddr([]byte("lockA"))}
		dbB, contentsB := "dbB", manifestContents{lock: computeAddr([]byte("lockB"))}

		c.Put(dbA, contentsA, t1)
		c.Put(dbB, contentsB, t1)

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
			c.Put(v, manifestContents{}, time.Now())
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
		c.Put("novel", manifestContents{}, time.Now())
		_, _, ok = c.Get(keys[lru])
		assert.False(ok)
		// |keys[lru]| is gone, so |keys[lru+1]| is next
		lru++

		// Putting a bigger value will dump multiple existing entries
		c.Put("big", manifestContents{vers: "big version"}, time.Now())
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
		c.Put("db", manifestContents{}, time.Now())
		_, _, ok := c.Get("db")
		assert.False(t, ok)
	})

	t.Run("ZeroSizeCache", func(t *testing.T) {
		c := newManifestCache(0)
		c.Put("db", manifestContents{}, time.Now())
		_, _, ok := c.Get("db")
		assert.False(t, ok)
	})

}
