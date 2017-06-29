// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"container/list"
	"time"

	"sync"

	"github.com/attic-labs/noms/go/d"
)

func newManifestCache(maxSize uint64) *manifestCache {
	return &manifestCache{
		maxSize: maxSize,
		cache:   map[string]manifestCacheEntry{},
		locked:  map[string]struct{}{},
		cond:    sync.NewCond(&sync.Mutex{}),
	}
}

type manifestCacheEntry struct {
	lruEntry *list.Element
	contents manifestContents
	t        time.Time
}

type manifestCache struct {
	totalSize uint64
	maxSize   uint64
	lru       list.List
	cache     map[string]manifestCacheEntry
	locked    map[string]struct{}
	cond      *sync.Cond
}

// Get() checks the searches the cache for an entry. If it exists, it moves it's
// lru entry to the back of the queue and returns (value, true). Otherwise, it
// returns (nil, false).
func (mc *manifestCache) Get(db string) (contents manifestContents, t time.Time, present bool) {
	mc.cond.L.Lock()
	defer mc.cond.L.Unlock()

	if entry, ok := mc.entry(db); ok {
		contents, t, present = entry.contents, entry.t, true
	}
	return
}

func (mc *manifestCache) Lock(db string) {
	mc.cond.L.Lock()
	defer mc.cond.L.Unlock()

	for {
		_, ok := mc.locked[db]
		if ok {
			mc.cond.Wait()
		} else {
			mc.locked[db] = struct{}{}
			break
		}
	}
}

func (mc *manifestCache) Unlock(db string) {
	mc.cond.L.Lock()
	defer mc.cond.L.Unlock()

	_, ok := mc.locked[db]
	d.PanicIfFalse(ok)
	delete(mc.locked, db)

	mc.cond.Broadcast()
}

// entry() checks if the value is in the cache. If not in the cache, it returns an
// empty manifestCacheEntry and false. It it is in the cache, it moves it to
// to the back of lru and returns the entry and true.
func (mc *manifestCache) entry(key string) (manifestCacheEntry, bool) {
	entry, ok := mc.cache[key]
	if !ok {
		return manifestCacheEntry{}, false
	}
	mc.lru.MoveToBack(entry.lruEntry)
	return entry, true
}

// Put inserts |contents| into the cache with the key |db|, replacing any
// currently cached value. Put() will add this element to the cache at the
// back of the queue as long it's size does not exceed maxSize. If the
// addition of this entry causes the size of the cache to exceed maxSize, the
// necessary entries at the front of the queue will be deleted in order to
// keep the total cache size below maxSize.
func (mc *manifestCache) Put(db string, contents manifestContents) {
	mc.cond.L.Lock()
	defer mc.cond.L.Unlock()

	if entry, ok := mc.entry(db); ok {
		mc.totalSize -= entry.contents.size()
		mc.lru.Remove(entry.lruEntry)
		delete(mc.cache, db)
	}

	if contents.size() <= mc.maxSize {
		newEl := mc.lru.PushBack(db)
		ce := manifestCacheEntry{lruEntry: newEl, contents: contents, t: time.Now()}
		mc.cache[db] = ce
		mc.totalSize += ce.contents.size()
		for el := mc.lru.Front(); el != nil && mc.totalSize > mc.maxSize; {
			key1 := el.Value.(string)
			ce, ok := mc.cache[key1]
			if !ok {
				d.Panic("manifestCache is missing expected value")
			}
			next := el.Next()
			delete(mc.cache, key1)
			mc.totalSize -= ce.contents.size()
			mc.lru.Remove(el)
			el = next
		}
	}
}
