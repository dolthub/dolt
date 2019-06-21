// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"os"
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/d"
)

func newFDCache(targetSize int) *fdCache {
	return &fdCache{targetSize: targetSize, cache: map[string]fdCacheEntry{}}
}

// fdCache ref-counts open file descriptors, but doesn't keep a hard cap on
// the number of open files. Once the cache's target size is exceeded, opening
// a new file causes the cache to try to get the cache back to the target size
// by closing fds with zero refs. If there aren't enough such fds, fdCache
// gives up and tries again next time a caller refs a file.
type fdCache struct {
	targetSize int
	mu         sync.Mutex
	cache      map[string]fdCacheEntry
}

type fdCacheEntry struct {
	refCount uint32
	f        *os.File
}

// RefFile returns an opened *os.File for the file at |path|, or an error
// indicating why the file could not be opened. If the cache already had an
// entry for |path|, RefFile increments its refcount and returns the cached
// pointer. If not, it opens the file and caches the pointer for others to
// use. If RefFile returns an error, it's guaranteed that no refCounts were
// changed, so it's an error to make a subsequent call to UnrefFile().
// This is intended for clients that hold fds for extremely short periods.
func (fc *fdCache) RefFile(path string) (f *os.File, err error) {
	refFile := func() *os.File {
		if ce, present := fc.cache[path]; present {
			ce.refCount++
			fc.cache[path] = ce
			return ce.f
		}
		return nil
	}

	f = func() *os.File {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return refFile()
	}()
	if f != nil {
		return f, nil
	}

	// Very much want this to be outside the lock, but the downside is that multiple callers may get here concurrently. That means we need to deal with the raciness below.
	f, err = os.Open(path)
	if err != nil {
		return nil, err
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if cached := refFile(); cached != nil {
		// Someone beat us to it, so close f and return cached fd
		f.Close()
		return cached, nil
	}
	// I won the race!
	fc.cache[path] = fdCacheEntry{f: f, refCount: 1}
	return f, nil
}

// UnrefFile reduces the refcount of the entry at |path|. If the cache is over
// |fc.targetSize|, UnrefFile makes a best effort to shrink the cache by dumping
// entries with a zero refcount. If there aren't enough zero refcount entries
// to drop to get the cache back to |fc.targetSize|, the cache will remain
// over |fc.targetSize| until the next call to UnrefFile().
func (fc *fdCache) UnrefFile(path string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if ce, present := fc.cache[path]; present {
		ce.refCount--
		fc.cache[path] = ce
	}
	if len(fc.cache) > fc.targetSize {
		// Sadly, we can't remove items from a map while iterating, so we'll record the stuff we want to drop and then do it after
		needed := len(fc.cache) - fc.targetSize
		toDrop := make([]string, 0, needed)
		for p, ce := range fc.cache {
			if ce.refCount != 0 {
				continue
			}
			toDrop = append(toDrop, p)
			err := ce.f.Close()
			d.PanicIfError(err)
			needed--
			if needed == 0 {
				break
			}
		}
		for _, p := range toDrop {
			delete(fc.cache, p)
		}
	}
}

// Drop dumps the entire cache and closes all currently open files.
func (fc *fdCache) Drop() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, ce := range fc.cache {
		ce.f.Close()
	}
	fc.cache = map[string]fdCacheEntry{}
}

// reportEntries is meant for testing.
func (fc *fdCache) reportEntries() sort.StringSlice {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	ret := make(sort.StringSlice, 0, len(fc.cache))
	for p := range fc.cache {
		ret = append(ret, p)
	}
	sort.Sort(ret)
	return ret
}
