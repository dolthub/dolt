// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"crypto/sha512"
	"strconv"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type manifest interface {
	// Name returns a stable, unique identifier for the store this manifest describes.
	Name() string

	// ParseIfExists extracts and returns values from a NomsBlockStore
	// manifest, if one exists. Concrete implementations are responsible for
	// defining how to find and parse the desired manifest, e.g. a
	// particularly-named file in a given directory. Implementations are also
	// responsible for managing whatever concurrency guarantees they require
	// for correctness. If the manifest exists, |exists| is set to true and
	// manifest data is returned, including the version of the Noms data in
	// the store, the root root hash.Hash of the store, and a tableSpec
	// describing every table that comprises the store.
	// If the manifest doesn't exist, |exists| is set to false and the other
	// return values are undefined. The |readHook| parameter allows race
	// condition testing. If it is non-nil, it will be invoked while the
	// implementation is guaranteeing exclusive access to the manifest.
	ParseIfExists(stats *Stats, readHook func()) (exists bool, contents manifestContents)

	manifestUpdater
}

type manifestUpdater interface {
	// Update optimistically tries to write a new manifest containing
	// |newContents|. If |lastLock| matches the lock hash in the currently
	// persisted manifest (logically, the lock that would be returned by
	// ParseIfExists), then Update succeeds and subsequent calls to both
	// Update and ParseIfExists will reflect a manifest containing
	// |newContents|. If not, Update fails. Regardless, the returned
	// manifestContents will reflect the current state of the world. Callers
	// should check that the returned root == the proposed root and, if not,
	// merge any desired new table information with the contents of the
	// returned []tableSpec before trying again.
	// Concrete implementations are responsible for ensuring that concurrent
	// Update calls (and ParseIfExists calls) are correct.
	// If writeHook is non-nil, it will be invoked while the implementation is
	// guaranteeing exclusive access to the manifest. This allows for testing
	// of race conditions.
	Update(lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents
}

type manifestContents struct {
	vers  string
	lock  addr
	root  hash.Hash
	specs []tableSpec
}

func (mc manifestContents) size() (size uint64) {
	size += uint64(len(mc.vers)) + addrSize + hash.ByteLen
	for _, sp := range mc.specs {
		size += uint64(len(sp.name)) + uint32Size // for sp.chunkCount
	}
	return
}

func newManifestLocks() *manifestLocks {
	return &manifestLocks{map[string]struct{}{}, map[string]struct{}{}, sync.NewCond(&sync.Mutex{})}
}

type manifestLocks struct {
	updating map[string]struct{}
	fetching map[string]struct{}
	cond     *sync.Cond
}

func (ml *manifestLocks) lockForFetch(db string) {
	lockByName(db, ml.cond, ml.fetching)
}

func (ml *manifestLocks) unlockForFetch(db string) {
	unlockByName(db, ml.cond, ml.fetching)
}

func (ml *manifestLocks) lockForUpdate(db string) {
	lockByName(db, ml.cond, ml.updating)
}

func (ml *manifestLocks) unlockForUpdate(db string) {
	unlockByName(db, ml.cond, ml.updating)
}

func lockByName(db string, c *sync.Cond, locks map[string]struct{}) {
	c.L.Lock()
	defer c.L.Unlock()

	for {
		if _, inProgress := locks[db]; !inProgress {
			locks[db] = struct{}{}
			break
		}
		c.Wait()
	}
}

func unlockByName(db string, c *sync.Cond, locks map[string]struct{}) {
	c.L.Lock()
	defer c.L.Unlock()

	_, ok := locks[db]
	d.PanicIfFalse(ok)
	delete(locks, db)

	c.Broadcast()
}

type manifestManager struct {
	m     manifest
	cache *manifestCache
	locks *manifestLocks
}

func (mm manifestManager) lockOutFetch() {
	mm.locks.lockForFetch(mm.Name())
}

func (mm manifestManager) allowFetch() {
	mm.locks.unlockForFetch(mm.Name())
}

func (mm manifestManager) LockForUpdate() {
	mm.locks.lockForUpdate(mm.Name())
}

func (mm manifestManager) UnlockForUpdate() {
	mm.locks.unlockForUpdate(mm.Name())
}

func (mm manifestManager) updateWillFail(lastLock addr) (cached manifestContents, doomed bool) {
	if upstream, _, hit := mm.cache.Get(mm.Name()); hit {
		if lastLock != upstream.lock {
			doomed, cached = true, upstream
		}
	}
	return
}

func (mm manifestManager) Fetch(stats *Stats) (exists bool, contents manifestContents) {
	entryTime := time.Now()

	mm.lockOutFetch()
	defer mm.allowFetch()

	cached, t, hit := mm.cache.Get(mm.Name())

	if hit && t.After(entryTime) {
		// Cache contains a manifest which is newer than entry time.
		return true, cached
	}

	t = time.Now()
	exists, contents = mm.m.ParseIfExists(stats, nil)
	mm.cache.Put(mm.Name(), contents, t)
	return
}

// Callers MUST protect uses of Update with Lock/UnlockForUpdate.
// Update does not call Lock/UnlockForUpdate() on its own because it is
// intended to be used in a larger critical section along with updateWillFail.
func (mm manifestManager) Update(lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents {
	if upstream, _, hit := mm.cache.Get(mm.Name()); hit {
		if lastLock != upstream.lock {
			return upstream
		}
	}
	t := time.Now()

	mm.lockOutFetch()
	defer mm.allowFetch()

	contents := mm.m.Update(lastLock, newContents, stats, writeHook)
	mm.cache.Put(mm.Name(), contents, t)
	return contents
}

func (mm manifestManager) Name() string {
	return mm.m.Name()
}

type tableSpec struct {
	name       addr
	chunkCount uint32
}

func parseSpecs(tableInfo []string) []tableSpec {
	specs := make([]tableSpec, len(tableInfo)/2)
	for i := range specs {
		specs[i].name = ParseAddr([]byte(tableInfo[2*i]))
		c, err := strconv.ParseUint(tableInfo[2*i+1], 10, 32)
		d.PanicIfError(err)
		specs[i].chunkCount = uint32(c)
	}
	return specs
}

func formatSpecs(specs []tableSpec, tableInfo []string) {
	d.Chk.True(len(tableInfo) == 2*len(specs))
	for i, t := range specs {
		tableInfo[2*i] = t.name.String()
		tableInfo[2*i+1] = strconv.FormatUint(uint64(t.chunkCount), 10)
	}
}

// generateLockHash returns a hash of root and the names of all the tables in
// specs, which should be included in all persisted manifests. When a client
// attempts to update a manifest, it must check the lock hash in the currently
// persisted manifest against the lock hash it saw last time it loaded the
// contents of a manifest. If they do not match, the client must not update
// the persisted manifest.
func generateLockHash(root hash.Hash, specs []tableSpec) (lock addr) {
	blockHash := sha512.New()
	blockHash.Write(root[:])
	for _, spec := range specs {
		blockHash.Write(spec.name[:])
	}
	var h []byte
	h = blockHash.Sum(h) // Appends hash to h
	copy(lock[:], h)
	return
}
