// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package sizecache

// SizeCache implements a simple LRU cache of interface{}-typed key-value pairs.
// When items are added, the "size" of the item must be provided. LRU items will
// be expired until the total of all items is below the specified size for the
// SizeCache
import (
	"container/list"
	"sync"

	"github.com/attic-labs/noms/go/d"
)

type sizeCacheEntry struct {
	size     uint64
	lruEntry *list.Element
	value    interface{}
}

type SizeCache struct {
	totalSize uint64
	maxSize   uint64
	mu        sync.Mutex
	lru       list.List
	cache     map[interface{}]sizeCacheEntry
	expireCb  func(elm interface{})
}

type ExpireCallback func(key interface{})

// New creates a SizeCache that will hold up to |maxSize| item data.
func New(maxSize uint64) *SizeCache {
	return NewWithExpireCallback(maxSize, nil)
}

// NewWithExpireCallback creates a SizeCache that will hold up to |maxSize|
// item data, and will call cb(key) when the item corresponding with that key
// expires.
func NewWithExpireCallback(maxSize uint64, cb ExpireCallback) *SizeCache {
	return &SizeCache{
		maxSize:  maxSize,
		cache:    map[interface{}]sizeCacheEntry{},
		expireCb: cb,
	}
}

// entry() checks if the value is in the cache. If not in the cache, it returns an
// empty sizeCacheEntry and false. It it is in the cache, it moves it to
// to the back of lru and returns the entry and true.
// Callers should have locked down the |c| with a call to c.mu.Lock() before
// calling this entry().
func (c *SizeCache) entry(key interface{}) (sizeCacheEntry, bool) {
	entry, ok := c.cache[key]
	if !ok {
		return sizeCacheEntry{}, false
	}
	c.lru.MoveToBack(entry.lruEntry)
	return entry, true
}

// Get checks the searches the cache for an entry. If it exists, it moves it's
// lru entry to the back of the queue and returns (value, true). Otherwise, it
// returns (nil, false).
func (c *SizeCache) Get(key interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entry(key); ok {
		return entry.value, true
	}
	return nil, false
}

// Add will add this element to the cache at the back of the queue as long it's
// size does not exceed maxSize. If the addition of this entry causes the size of
// the cache to exceed maxSize, the necessary entries at the front of the queue
// will be deleted in order to keep the total cache size below maxSize.
func (c *SizeCache) Add(key interface{}, size uint64, value interface{}) {
	if size <= c.maxSize {
		c.mu.Lock()
		defer c.mu.Unlock()

		if _, ok := c.entry(key); ok {
			// this value is already in the cache; just return
			return
		}

		newEl := c.lru.PushBack(key)
		ce := sizeCacheEntry{size: size, lruEntry: newEl, value: value}
		c.cache[key] = ce
		c.totalSize += ce.size
		for el := c.lru.Front(); el != nil && c.totalSize > c.maxSize; {
			key1 := el.Value
			ce, ok := c.cache[key1]
			if !ok {
				d.Panic("SizeCache is missing expected value")
			}
			next := el.Next()
			delete(c.cache, key1)
			c.totalSize -= ce.size
			c.lru.Remove(el)
			if c.expireCb != nil {
				c.expireCb(key1)
			}
			el = next
		}
	}
}

// Drop will remove the element associated with the given key from the cache.
func (c *SizeCache) Drop(key interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entry(key); ok {
		c.totalSize -= entry.size
		c.lru.Remove(entry.lruEntry)
		delete(c.cache, key)
	}
}
