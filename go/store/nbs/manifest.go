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

package nbs

import (
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrCorruptManifest = errors.New("corrupt manifest")
var ErrUnsupportedManifestAppendixOption = errors.New("unsupported manifest appendix option")

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
	ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (exists bool, contents manifestContents, err error)

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
	Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error)
}

type manifestGCGenUpdater interface {
	// UpdateGCGen tries to write a new manifest containing |newContents|.
	// Like Update(), it requires that |lastLock| matches the currently persisted
	// lock hash. However, unlike Update() |newContents.root| must remain the same,
	// while |newContents.gcGen| must be updated to a new value.
	// Concrete implementations are responsible for ensuring that concurrent
	// Update calls (and ParseIfExists calls) are correct.
	// If writeHook is non-nil, it will be invoked while the implementation is
	// guaranteeing exclusive access to the manifest. This allows for testing
	// of race conditions.
	UpdateGCGen(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error)
}

// ManifestInfo is an interface for retrieving data from a manifest outside of this package
type ManifestInfo interface {
	GetVersion() string
	GetLock() string
	GetGCGen() string
	GetRoot() hash.Hash
	NumTableSpecs() int
	NumAppendixSpecs() int
	GetTableSpecInfo(i int) TableSpecInfo
	GetAppendixTableSpecInfo(i int) TableSpecInfo
}

type ManifestAppendixOption int

const (
	ManifestAppendixOption_Unspecified ManifestAppendixOption = iota
	ManifestAppendixOption_Set
	ManifestAppendixOption_Append
)

type manifestContents struct {
	manifestVers string
	nbfVers      string
	lock         hash.Hash
	root         hash.Hash
	gcGen        hash.Hash
	specs        []tableSpec

	// An appendix is a list of |tableSpecs| that track an auxiliary collection of
	// table files used _only_ for query performance optimizations. These appendix |tableSpecs| can be safely
	// managed with nbs.UpdateManifestWithAppendix, however generation and removal of the actual table files
	// the appendix |tableSpecs| reference is done manually. All appendix |tableSpecs| will be prepended to the
	// manifest.specs across manifest updates.
	appendix []tableSpec
}

// GetVersion returns the noms binary format of the manifest
func (mc manifestContents) GetVersion() string {
	return mc.nbfVers
}

func (mc manifestContents) GetLock() string {
	return mc.lock.String()
}

func (mc manifestContents) GetGCGen() string {
	return mc.gcGen.String()
}

func (mc manifestContents) GetRoot() hash.Hash {
	return mc.root
}

func (mc manifestContents) NumTableSpecs() int {
	return len(mc.specs)
}

func (mc manifestContents) NumAppendixSpecs() int {
	return len(mc.appendix)
}

func (mc manifestContents) GetTableSpecInfo(i int) TableSpecInfo {
	return mc.specs[i]
}

func (mc manifestContents) GetAppendixTableSpecInfo(i int) TableSpecInfo {
	return mc.appendix[i]
}

func (mc manifestContents) getSpec(i int) tableSpec {
	return mc.specs[i]
}

func (mc manifestContents) getAppendixSpec(i int) tableSpec {
	return mc.appendix[i]
}

func (mc manifestContents) removeAppendixSpecs() (manifestContents, []tableSpec) {
	if mc.appendix == nil || len(mc.appendix) == 0 {
		return mc, nil
	}

	appendixSet := mc.getAppendixSet()
	filtered := make([]tableSpec, 0)
	removed := make([]tableSpec, 0)
	for _, s := range mc.specs {
		if _, ok := appendixSet[s.hash]; ok {
			removed = append(removed, s)
		} else {
			filtered = append(filtered, s)
		}
	}

	return manifestContents{
		nbfVers: mc.nbfVers,
		lock:    mc.lock,
		root:    mc.root,
		gcGen:   mc.gcGen,
		specs:   filtered,
	}, removed
}

func (mc manifestContents) getSpecSet() (ss map[hash.Hash]struct{}) {
	return toSpecSet(mc.specs)
}

func (mc manifestContents) getAppendixSet() (ss map[hash.Hash]struct{}) {
	return toSpecSet(mc.appendix)
}

func toSpecSet(specs []tableSpec) (ss map[hash.Hash]struct{}) {
	ss = make(map[hash.Hash]struct{}, len(specs))
	for _, ts := range specs {
		ss[ts.hash] = struct{}{}
	}
	return ss
}

func (mc manifestContents) size() (size uint64) {
	size += uint64(len(mc.nbfVers)) + hash.ByteLen + hash.ByteLen
	for _, sp := range mc.specs {
		size += uint64(len(sp.hash)) + uint32Size // for sp.chunkCount
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

func (ml *manifestLocks) unlockForFetch(db string) error {
	return unlockByName(db, ml.cond, ml.fetching)
}

func (ml *manifestLocks) lockForUpdate(db string) {
	lockByName(db, ml.cond, ml.updating)
}

func (ml *manifestLocks) unlockForUpdate(db string) error {
	return unlockByName(db, ml.cond, ml.updating)
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

func unlockByName(db string, c *sync.Cond, locks map[string]struct{}) error {
	c.L.Lock()
	defer c.L.Unlock()

	if _, ok := locks[db]; !ok {
		return errors.New("unlock failed")
	}

	delete(locks, db)

	c.Broadcast()

	return nil
}

type manifestManager struct {
	m     manifest
	cache *manifestCache
	locks *manifestLocks
}

func (mm manifestManager) lockOutFetch() {
	mm.locks.lockForFetch(mm.Name())
}

func (mm manifestManager) allowFetch() error {
	return mm.locks.unlockForFetch(mm.Name())
}

func (mm manifestManager) LockForUpdate() {
	mm.locks.lockForUpdate(mm.Name())
}

func (mm manifestManager) UnlockForUpdate() error {
	return mm.locks.unlockForUpdate(mm.Name())
}

func (mm manifestManager) updateWillFail(lastLock hash.Hash) (cached manifestContents, doomed bool) {
	if upstream, _, hit := mm.cache.Get(mm.Name()); hit {
		if lastLock != upstream.lock {
			doomed, cached = true, upstream
		}
	}
	return
}

func (mm manifestManager) Fetch(ctx context.Context, stats *Stats) (exists bool, contents manifestContents, t time.Time, err error) {
	entryTime := time.Now()

	mm.lockOutFetch()
	defer func() {
		afErr := mm.allowFetch()

		if err == nil {
			err = afErr
		}
	}()

	f := func() (bool, manifestContents, time.Time, error) {
		cached, t, hit := mm.cache.Get(mm.Name())

		if hit && t.After(entryTime) {
			// Cache contains a manifest which is newer than entry time.
			return true, cached, t, nil
		}

		t = time.Now()

		exists, contents, err := mm.m.ParseIfExists(ctx, stats, nil)

		if err != nil {
			return false, manifestContents{}, t, err
		}

		err = mm.cache.Put(mm.Name(), contents, t)

		if err != nil {
			return false, manifestContents{}, t, err
		}

		return exists, contents, t, nil
	}

	exists, contents, t, err = f()
	return
}

// Update attempts to write a new manifest.
// Callers MUST protect uses of Update with Lock/UnlockForUpdate.
// Update does not call Lock/UnlockForUpdate() on its own because it is
// intended to be used in a larger critical section along with updateWillFail.
func (mm manifestManager) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (contents manifestContents, err error) {
	if upstream, _, hit := mm.cache.Get(mm.Name()); hit {
		if lastLock != upstream.lock {
			return upstream, nil
		}
	}
	t := time.Now()

	mm.lockOutFetch()
	defer func() {
		afErr := mm.allowFetch()

		if err == nil {
			err = afErr
		}
	}()

	f := func() (manifestContents, error) {
		contents, err := mm.m.Update(ctx, lastLock, newContents, stats, writeHook)

		if err != nil {
			return contents, err
		}

		err = mm.cache.Put(mm.Name(), contents, t)

		if err != nil {
			return manifestContents{}, err
		}

		return contents, nil
	}

	contents, err = f()
	return
}

// UpdateGCGen will update the manifest with a new garbage collection generation.
// Callers MUST protect uses of UpdateGCGen with Lock/UnlockForUpdate.
func (mm manifestManager) UpdateGCGen(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (contents manifestContents, err error) {
	updater, ok := mm.m.(manifestGCGenUpdater)
	if !ok {
		return manifestContents{}, errors.New("manifest does not support updating gc gen")
	}

	if upstream, _, hit := mm.cache.Get(mm.Name()); hit {
		if lastLock != upstream.lock {
			return manifestContents{}, errors.New("manifest was modified during garbage collection")
		}
	}
	t := time.Now()

	mm.lockOutFetch()
	defer func() {
		afErr := mm.allowFetch()

		if err == nil {
			err = afErr
		}
	}()

	f := func() (manifestContents, error) {
		contents, err := updater.UpdateGCGen(ctx, lastLock, newContents, stats, writeHook)

		if err != nil {
			return contents, err
		}

		err = mm.cache.Put(mm.Name(), contents, t)

		if err != nil {
			return manifestContents{}, err
		}

		return contents, nil
	}

	contents, err = f()
	return
}

func (mm manifestManager) Close() error {
	mm.cache.Delete(mm.Name())
	return nil
}

func (mm manifestManager) Name() string {
	return mm.m.Name()
}

// TableSpecInfo is an interface for retrieving data from a tableSpec outside of this package
type TableSpecInfo interface {
	GetFileName() string
	GetChunkCount() uint32
}

type tableFileType int

const (
	typeNoms tableFileType = iota
	typeArchive
)

type tableSpec struct {
	fileType   tableFileType
	hash       hash.Hash
	chunkCount uint32
}

func (ts tableSpec) GetFileName() string {
	switch ts.fileType {
	case typeNoms:
		return ts.hash.String()
	case typeArchive:
		return ts.hash.String() + ".darc" // NM4 - common code for this???
	default:
		panic(fmt.Sprintf("runtime error: unknown table file type: %d", ts.fileType))
	}
}

func (ts tableSpec) GetChunkCount() uint32 {
	return ts.chunkCount
}

func tableSpecsToMap(specs []tableSpec) map[string]int {
	m := make(map[string]int)
	for _, spec := range specs {
		m[spec.hash.String()] = int(spec.chunkCount)
	}

	return m
}

func parseSpecs(tableInfo []string) ([]tableSpec, error) {
	specs := make([]tableSpec, len(tableInfo)/2)
	for i := range specs {
		var err error
		var ok bool
		specs[i].hash, ok = hash.MaybeParse(tableInfo[2*i])
		if !ok {
			return nil, fmt.Errorf("invalid table file name: %s", tableInfo[2*i])
		}

		c, err := strconv.ParseUint(tableInfo[2*i+1], 10, 32)

		if err != nil {
			return nil, err
		}

		specs[i].chunkCount = uint32(c)
	}

	return specs, nil
}

func formatSpecs(specs []tableSpec, tableInfo []string) {
	d.Chk.True(len(tableInfo) == 2*len(specs))
	for i, t := range specs {
		tableInfo[2*i] = t.hash.String()
		tableInfo[2*i+1] = strconv.FormatUint(uint64(t.chunkCount), 10)
	}
}

// generateLockHash returns a hash of root and the names of all the tables in
// specs, which should be included in all persisted manifests. When a client
// attempts to update a manifest, it must check the lock hash in the currently
// persisted manifest against the lock hash it saw last time it loaded the
// contents of a manifest. If they do not match, the client must not update
// the persisted manifest.
func generateLockHash(root hash.Hash, specs []tableSpec, appendix []tableSpec, extra []byte) hash.Hash {
	blockHash := sha512.New()
	blockHash.Write(root[:])
	for _, spec := range appendix {
		blockHash.Write(spec.hash[:])
	}
	blockHash.Write([]byte{0})
	for _, spec := range specs {
		blockHash.Write(spec.hash[:])
	}
	if len(extra) > 0 {
		blockHash.Write([]byte{0})
		blockHash.Write(extra)
	}
	blockHash.Write([]byte{0})
	var h []byte
	h = blockHash.Sum(h) // Appends hash to h
	return hash.New(h[:hash.ByteLen])
}
