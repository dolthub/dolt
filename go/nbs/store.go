// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	StorageVersion = "0"

	defaultMemTableSize uint64 = 512 * 1 << 20 // 512MB
)

type NomsBlockStore struct {
	mm          fileManifest
	tm          tableManager
	nomsVersion string

	mu        sync.RWMutex // protects the following state
	mt        *memTable
	mtSize    uint64
	immTables chunkSources // slice is never mutated. on change, new slice is constructed and assigned
	root      hash.Hash

	putCount uint64
}

type chunkSources []chunkSource

func (css chunkSources) has(h addr) bool {
	for _, haver := range css {
		if haver.has(h) {
			return true
		}
	}
	return false
}

func (css chunkSources) hasMany(addrs []hasRecord) (remaining bool) {
	for _, haver := range css {
		if !haver.hasMany(addrs) {
			return false
		}
	}
	return true
}

func (css chunkSources) get(h addr) []byte {
	for _, haver := range css {
		if data := haver.get(h); data != nil {
			return data
		}
	}
	return nil
}

func (css chunkSources) getMany(reqs []getRecord) (remaining bool) {
	for _, haver := range css {
		if !haver.getMany(reqs) {
			return false
		}
	}

	return true
}

func NewBlockStore(dir string, memTableSize uint64) *NomsBlockStore {
	return hookedNewNomsBlockStore(dir, memTableSize, nil)
}

func hookedNewNomsBlockStore(dir string, memTableSize uint64, readHook func()) *NomsBlockStore {
	if memTableSize == 0 {
		memTableSize = defaultMemTableSize
	}
	nbs := &NomsBlockStore{
		mm:          fileManifest{dir},
		tm:          &fileTableManager{dir},
		nomsVersion: constants.NomsVersion,
		mtSize:      memTableSize,
	}

	if exists, vers, root, tableSpecs := nbs.mm.ParseIfExists(readHook); exists {
		nbs.nomsVersion, nbs.root = vers, root
		nbs.immTables = unionTables(nil, nbs.tm, tableSpecs)
	}

	return nbs
}

// unionTables returns a new chunkSources that contains all of curTables as well as a chunkSource for each as-yet-unknown table named by tableSpecs.
func unionTables(curTables chunkSources, tm tableManager, tableSpecs []tableSpec) chunkSources {
	newTables := make(chunkSources, len(curTables))
	known := map[addr]struct{}{}
	for i, t := range curTables {
		known[t.hash()] = struct{}{}
		newTables[i] = curTables[i]
	}

	for _, t := range tableSpecs {
		if _, present := known[t.name]; !present {
			newTables = append(newTables, tm.open(t.name, t.chunkCount))
		}
	}
	return newTables
}

func (nbs *NomsBlockStore) Put(c chunks.Chunk) {
	a := addr(c.Hash().Digest())
	d.PanicIfFalse(nbs.addChunk(a, c.Data()))
	nbs.putCount++
}

func (nbs *NomsBlockStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
	nbs.Put(c)
}

func (nbs *NomsBlockStore) PutMany(chunx []chunks.Chunk) (err chunks.BackpressureError) {
	for ; len(chunx) > 0; chunx = chunx[1:] {
		c := chunx[0]
		a := addr(c.Hash().Digest())
		if !nbs.addChunk(a, c.Data()) {
			break
		}
		nbs.putCount++
	}
	for _, c := range chunx {
		err = append(err, c.Hash())
	}
	return err
}

func (nbs *NomsBlockStore) addChunk(h addr, data []byte) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.mt == nil {
		nbs.mt = newMemTable(nbs.mtSize)
	}
	if !nbs.mt.addChunk(h, data) {
		if tableHash, chunkCount := nbs.tm.compact(nbs.mt, nbs.immTables); chunkCount > 0 {
			nbs.immTables = prependTable(nbs.immTables, nbs.tm.open(tableHash, chunkCount))
		}
		nbs.mt = newMemTable(nbs.mtSize)
		return nbs.mt.addChunk(h, data)
	}
	return true
}

func prependTable(curTables chunkSources, crc chunkSource) chunkSources {
	newTables := make(chunkSources, len(curTables)+1)
	newTables[0] = crc
	copy(newTables[1:], curTables)
	return newTables
}

func (nbs *NomsBlockStore) Get(h hash.Hash) chunks.Chunk {
	a := addr(h.Digest())
	data, tables := func() (data []byte, tables chunkSources) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			data = nbs.mt.get(a)
		}
		return data, nbs.immTables
	}()
	if data != nil {
		return chunks.NewChunkWithHash(h, data)
	}
	if data := tables.get(a); data != nil {
		return chunks.NewChunkWithHash(h, data)
	}
	return chunks.EmptyChunk
}

func (nbs *NomsBlockStore) GetMany(hashes []hash.Hash) []chunks.Chunk {
	reqs := make([]getRecord, len(hashes))
	for i, h := range hashes {
		a := addr(h.Digest())
		reqs[i] = getRecord{
			a:      &a,
			prefix: a.Prefix(),
			order:  i,
		}
	}

	tables, remaining := func() (tables chunkSources, remaining bool) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.immTables

		if nbs.mt != nil {
			remaining = nbs.mt.getMany(reqs)
		} else {
			remaining = true
		}

		return
	}()

	sort.Sort(getRecordByPrefix(reqs))

	if remaining {
		tables.getMany(reqs)
	}

	sort.Sort(getRecordByOrder(reqs))

	resp := make([]chunks.Chunk, len(hashes))
	for i, req := range reqs {
		if req.data == nil {
			resp[i] = chunks.EmptyChunk
		} else {
			resp[i] = chunks.NewChunkWithHash(hashes[i], req.data)
		}
	}

	return resp
}

func (nbs *NomsBlockStore) Has(h hash.Hash) bool {
	a := addr(h.Digest())
	has, tables := func() (bool, chunkSources) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		return nbs.mt != nil && nbs.mt.has(a), nbs.immTables
	}()
	return has || tables.has(a)
}

func (nbs *NomsBlockStore) Root() hash.Hash {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.root
}

func (nbs *NomsBlockStore) UpdateRoot(current, last hash.Hash) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	d.Chk.True(nbs.root == last, "UpdateRoot: last != nbs.Root(); %s != %s", last, nbs.root)

	if nbs.mt != nil && nbs.mt.count() > 0 {
		if tableHash, chunkCount := nbs.tm.compact(nbs.mt, nbs.immTables); chunkCount > 0 {
			nbs.immTables = prependTable(nbs.immTables, nbs.tm.open(tableHash, chunkCount))
		}
		nbs.mt = nil
	}

	actual, tableNames := nbs.mm.Update(nbs.immTables, nbs.root, current, nil)

	if current != actual {
		nbs.root = actual
		nbs.immTables = unionTables(nbs.immTables, nbs.tm, tableNames)
		return false
	}
	nbs.nomsVersion, nbs.root = constants.NomsVersion, current
	return true
}

func (nbs *NomsBlockStore) Version() string {
	return nbs.nomsVersion
}

func (nbs *NomsBlockStore) Close() (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	for _, t := range nbs.immTables {
		err = t.close() // TODO: somehow coalesce these errors??
	}
	return
}

// types.BatchStore
func (nbs *NomsBlockStore) AddHints(hints types.Hints) {
	// noop
}

func (nbs *NomsBlockStore) Flush() {
	// noop
}
