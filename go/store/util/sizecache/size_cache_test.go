// Copyright 2019 Dolthub, Inc.
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

package sizecache

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/hash"
)

func hashFromString(s string) hash.Hash {
	return hash.Of([]byte(s))
}

func TestSizeCache(t *testing.T) {
	assert := assert.New(t)
	defSize := uint64(200)

	c := New(1024)
	for i, v := range []string{"data-1", "data-2", "data-3", "data-4", "data-5", "data-6", "data-7", "data-8", "data-9"} {
		c.Add(hashFromString(v), defSize, v)
		maxElements := uint64(i + 1)
		if maxElements >= uint64(5) {
			maxElements = uint64(5)
		}
		assert.Equal(maxElements*defSize, c.totalSize)
	}

	_, ok := c.Get(hashFromString("data-1"))
	assert.False(ok)
	assert.Equal(hashFromString("data-5"), c.lru.Front().Value)

	v, ok := c.Get(hashFromString("data-5"))
	assert.True(ok)
	assert.Equal("data-5", v.(string))
	assert.Equal(hashFromString("data-5"), c.lru.Back().Value)
	assert.Equal(hashFromString("data-6"), c.lru.Front().Value)

	c.Add(hashFromString("data-7"), defSize, "data-7")
	assert.Equal(hashFromString("data-7"), c.lru.Back().Value)
	assert.Equal(uint64(1000), c.totalSize)

	c.Add(hashFromString("no-data"), 0, nil)
	v, ok = c.Get(hashFromString("no-data"))
	assert.True(ok)
	assert.Nil(v)
	assert.Equal(hashFromString("no-data"), c.lru.Back().Value)
	assert.Equal(uint64(1000), c.totalSize)
	assert.Equal(6, c.lru.Len())
	assert.Equal(6, len(c.cache))

	for _, v := range []string{"data-5", "data-6", "data-7", "data-8", "data-9"} {
		c.Get(hashFromString(v))
		assert.Equal(hashFromString(v), c.lru.Back().Value)
	}
	assert.Equal(hashFromString("no-data"), c.lru.Front().Value)

	c.Add(hashFromString("data-10"), 200, "data-10")
	assert.Equal(uint64(1000), c.totalSize)
	assert.Equal(5, c.lru.Len())
	assert.Equal(5, len(c.cache))

	_, ok = c.Get(hashFromString("no-data"))
	assert.False(ok)
	_, ok = c.Get(hashFromString("data-5"))
	assert.False(ok)

	c.Drop(hashFromString("data-10"))
	assert.Equal(uint64(800), c.totalSize)
	assert.Equal(4, c.lru.Len())
	assert.Equal(4, len(c.cache))

	c.Purge()
	assert.Equal(uint64(0), c.totalSize)
	for i, v := range []string{"data-1", "data-2", "data-3", "data-4", "data-5", "data-6", "data-7", "data-8", "data-9"} {
		c.Add(hashFromString(v), defSize, v)
		maxElements := uint64(i + 1)
		if maxElements >= uint64(5) {
			maxElements = uint64(5)
		}
		assert.Equal(maxElements*defSize, c.totalSize)
	}
}

func TestSizeCacheWithExpiry(t *testing.T) {
	expired := []string{}
	expire := func(key interface{}) {
		expired = append(expired, key.(string))
	}

	c := NewWithExpireCallback(5, expire)
	data := []string{"a", "b", "c", "d", "e"}
	for i, k := range data {
		c.Add(k, 1, i)
	}

	c.Add("big", 5, "thing")
	sort.Strings(expired)
	assert.Equal(t, data, expired)
}

func concurrencySizeCacheTest(data []string) {
	dchan := make(chan string, 128)
	go func() {
		for _, d := range data {
			dchan <- d
		}
		close(dchan)
	}()

	cache := New(25)
	wg := sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			for d := range dchan {
				cache.Add(d, uint64(len(d)), d)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// I can't guarantee this will fail if the code isn't correct, but in the
// previous version of SizeCache, this was able to reliably repro bug #2663.
func TestConcurrency(t *testing.T) {
	assert := assert.New(t)
	generateDataStrings := func(numStrings, numValues int) []string {
		l := []string{}
		for i := 0; len(l) < numStrings; i++ {
			for j := 0; j < numValues && len(l) < numStrings; j++ {
				l = append(l, fmt.Sprintf("data-%d", i))
			}
		}
		return l
	}

	data := generateDataStrings(50, 3)
	for i := 0; i < 100; i++ {
		assert.NotPanics(func() { concurrencySizeCacheTest(data) })
	}
}

func TestTooLargeValue(t *testing.T) {
	assert := assert.New(t)

	c := New(1024)
	c.Add(hashFromString("big-data"), 2048, "big-data")
	_, ok := c.Get(hashFromString("big-data"))
	assert.False(ok)
}

func TestZeroSizeCache(t *testing.T) {
	assert := assert.New(t)

	c := New(0)
	c.Add(hashFromString("data1"), 200, "data1")
	_, ok := c.Get(hashFromString("data1"))
	assert.False(ok)
}
