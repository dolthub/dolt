// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package sizecache

// SizeCache implements a simple LRU cache of interface{}-typed key-value pairs. When items are added, the "size" of the item must be provided. LRU items will be expired until the total of all items is below the specified size for the SizeCache
import (
	"container/list"
	"sync"

	"github.com/attic-labs/noms/go/d"
)

type lruList struct {
	list.List
}

type sizeCacheEntry struct {
	size     uint64
	lruEntry *list.Element
	value    interface{}
}

type SizeCache struct {
	totalSize uint64
	maxSize   uint64
	mu        sync.Mutex
	lru       lruList
	cache     map[interface{}]sizeCacheEntry
}

func New(maxSize uint64) *SizeCache {
	return &SizeCache{maxSize: maxSize, cache: map[interface{}]sizeCacheEntry{}}
}

func (c *SizeCache) entry(key interface{}) (sizeCacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.cache[key]
	if !ok {
		return sizeCacheEntry{}, false
	}
	c.lru.MoveToBack(entry.lruEntry)
	return entry, true
}

func (c *SizeCache) Get(key interface{}) (interface{}, bool) {
	if entry, ok := c.entry(key); ok {
		return entry.value, true
	}
	return nil, false
}

func (c *SizeCache) Add(key interface{}, size uint64, value interface{}) {
	if size <= c.maxSize {
		if _, ok := c.entry(key); ok {
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		newEl := c.lru.PushBack(key)
		ce := sizeCacheEntry{size: size, lruEntry: newEl, value: value}
		c.cache[key] = ce
		c.totalSize += ce.size
		for el := c.lru.Front(); el != nil && c.totalSize > c.maxSize; {
			key1 := el.Value
			ce, ok := c.cache[key1]
			d.PanicIfFalse(ok, "SizeCache is missing expected value")
			next := el.Next()
			delete(c.cache, key1)
			c.totalSize -= ce.size
			c.lru.Remove(el)
			el = next
		}
	}
}
