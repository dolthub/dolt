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
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	_, _, _, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// No manifest file gets written until the first call to Commit(). Prior to that, Root() will simply return hash.Hash{}.
	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatDoltString, store.Version())
}

func TestChunkStoreVersion(t *testing.T) {
	assert := assert.New(t)
	_, _, _, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	assert.Equal(constants.FormatDoltString, store.Version())
	newChunk := chunks.NewChunk([]byte("new root"))
	require.NoError(t, store.Put(context.Background(), newChunk, noopGetAddrs))
	newRoot := newChunk.Hash()

	if assert.True(store.Commit(context.Background(), newRoot, hash.Hash{})) {
		assert.Equal(constants.FormatDoltString, store.Version())
	}
}

func TestChunkStoreRebase(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatDoltString, store.Version())

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatDoltString, store.Version())

	err = store.Rebase(context.Background())
	require.NoError(t, err)

	// NOW it should
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.FormatDoltString, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommit(t *testing.T) {
	assert := assert.New(t)
	_, _, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)

	newRootChunk := chunks.NewChunk([]byte("new root"))
	newRoot := newRootChunk.Hash()
	err = store.Put(context.Background(), newRootChunk, noopGetAddrs)
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), newRoot, hash.Hash{})
	require.NoError(t, err)
	if assert.True(success) {
		has, err := store.Has(context.Background(), newRoot)
		require.NoError(t, err)
		assert.True(has)
		h, err := store.Root(context.Background())
		require.NoError(t, err)
		assert.Equal(newRoot, h)
	}

	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	err = store.Put(context.Background(), secondRootChunk, noopGetAddrs)
	require.NoError(t, err)
	success, err = store.Commit(context.Background(), secondRoot, newRoot)
	require.NoError(t, err)
	if assert.True(success) {
		h, err := store.Root(context.Background())
		require.NoError(t, err)
		assert.Equal(secondRoot, h)
		has, err := store.Has(context.Background(), newRoot)
		require.NoError(t, err)
		assert.True(has)
		has, err = store.Has(context.Background(), secondRoot)
		require.NoError(t, err)
		assert.True(has)
	}
}

func TestChunkStoreManifestAppearsAfterConstruction(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatDoltString, store.Version())

	// Simulate another process writing a manifest behind store's back.
	interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatDoltString, store.Version())
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifest(fm)
	q := NewUnlimitedMemQuotaProvider()
	defer func() {
		require.EqualValues(t, 0, q.Usage())
	}()
	p := newFakeTablePersister(q)

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	store, err := newNomsBlockStore(context.Background(), constants.FormatDoltString, mm, p, q, inlineConjoiner{defaultMaxTables}, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.FormatDoltString, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommitOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	newChunk := chunks.NewChunk([]byte("new root 2"))
	require.NoError(t, store.Put(context.Background(), newChunk, noopGetAddrs))
	newRoot2 := newChunk.Hash()
	success, err := store.Commit(context.Background(), newRoot2, hash.Hash{})
	require.NoError(t, err)
	assert.False(success)
	assertDataInStore(chks, store, assert)
	success, err = store.Commit(context.Background(), newRoot2, newRoot)
	require.NoError(t, err)
	assert.True(success)
}

func TestChunkStoreManifestInProcessOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifest(fm)
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)

	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.FormatDoltString, mm, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	// Simulate another goroutine writing a manifest behind store's back.
	interloper, err := newNomsBlockStore(context.Background(), constants.FormatDoltString, mm, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, interloper.Close())
	}()

	chunk := chunks.NewChunk([]byte("hello"))
	err = interloper.Put(context.Background(), chunk, noopGetAddrs)
	require.NoError(t, err)
	assert.True(interloper.Commit(context.Background(), chunk.Hash(), hash.Hash{}))

	// Try to land a new chunk in store. The commit fails the optimistic lock
	// check (the interloper moved the root out from under us). store's memtable
	// is persisted to a novel table file while attempting the doomed update,
	// but the table is carried forward across the rebase so a subsequent commit
	// with the correct |last| succeeds.
	chunk = chunks.NewChunk([]byte("goodbye"))
	err = store.Put(context.Background(), chunk, noopGetAddrs)
	require.NoError(t, err)
	assert.NotNil(store.memtable)
	assert.False(store.Commit(context.Background(), chunk.Hash(), hash.Hash{}))

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), chunk.Hash(), h)
	require.NoError(t, err)
	assert.True(success)
	assert.Nil(store.memtable)

	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(chunk.Hash(), h)
	assert.Equal(constants.FormatDoltString, store.Version())
}

// TestChunkStoreConcurrentInProcessCommits exercises two NomsBlockStore
// instances backed by the same manifest committing concurrently. There is no
// longer an in-process manifest update lock serializing them; correctness rests
// entirely on the manifest's compare-and-swap on the lock hash plus each store's
// optimistic retry loop. Both stores' chunks must end up in the shared manifest.
func TestChunkStoreConcurrentInProcessCommits(t *testing.T) {
	fm := &fakeManifest{name: "foo"}
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)
	c := inlineConjoiner{defaultMaxTables}

	mkStore := func() *NomsBlockStore {
		s, err := newNomsBlockStore(context.Background(), constants.FormatDoltString, fm, p, q, c, defaultMemTableSize)
		require.NoError(t, err)
		return s
	}
	storeA := mkStore()
	defer storeA.Close()
	storeB := mkStore()
	defer storeB.Close()

	chunkA := chunks.NewChunk([]byte("chunk a"))
	chunkB := chunks.NewChunk([]byte("chunk b"))

	// addChunk adds |ch| to |s| without moving the root, retrying on optimistic
	// lock failure (re-reading the shared manifest via Rebase) until it lands.
	addChunk := func(s *NomsBlockStore, ch chunks.Chunk) error {
		ctx := context.Background()
		if err := s.Put(ctx, ch, noopGetAddrs); err != nil {
			return err
		}
		for {
			if err := s.Rebase(ctx); err != nil {
				return err
			}
			root, err := s.Root(ctx)
			if err != nil {
				return err
			}
			ok, err := s.Commit(ctx, root, root)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
	}

	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = addChunk(storeA, chunkA) }()
	go func() { defer wg.Done(); errs[1] = addChunk(storeB, chunkB) }()
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	require.NoError(t, storeA.Rebase(context.Background()))
	hasA, err := storeA.Has(context.Background(), chunkA.Hash())
	require.NoError(t, err)
	hasB, err := storeA.Has(context.Background(), chunkB.Hash())
	require.NoError(t, err)
	assert.True(t, hasA)
	assert.True(t, hasB)
}

func makeStoreWithFakes(t *testing.T) (fm *fakeManifest, p tablePersister, q MemoryQuotaProvider, store *NomsBlockStore) {
	fm = &fakeManifest{}
	mm := manifest(fm)
	q = NewUnlimitedMemQuotaProvider()
	p = newFakeTablePersister(q)
	store, err := newNomsBlockStore(context.Background(), constants.FormatDoltString, mm, p, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	return
}

// Simulate another process writing a manifest behind store's back.
func interloperWrite(fm *fakeManifest, p tablePersister, rootChunk []byte, chunks ...[]byte) (newRoot hash.Hash, persisted [][]byte, err error) {
	newLock, newRoot := computeAddr([]byte("locker")), hash.Of(rootChunk)
	persisted = append(chunks, rootChunk)

	var src chunkSource
	src, _, err = p.Persist(context.Background(), dherrors.FatalBehaviorError, createMemTable(persisted), nil, nil, &Stats{})
	if err != nil {
		return hash.Hash{}, nil, err
	}

	fm.set(constants.FormatDoltString, newLock, newRoot, []tableSpec{{src.hash(), uint32(len(chunks) + 1)}}, nil)

	if err = src.close(); err != nil {
		return [20]byte{}, nil, err
	}
	return
}

func createMemTable(chunks [][]byte) *memTable {
	mt := newMemTable(1 << 10)
	for _, c := range chunks {
		mt.addChunk(computeAddr(c), c)
	}
	return mt
}

func assertDataInStore(slices [][]byte, store chunks.ChunkStore, assert *assert.Assertions) {
	for _, data := range slices {
		ok, err := store.Has(context.Background(), chunks.NewChunk(data).Hash())
		assert.NoError(err)
		assert.True(ok)
	}
}

// fakeManifest simulates a fileManifest without touching disk.
type fakeManifest struct {
	name     string
	contents manifestContents
	mu       sync.RWMutex
}

func (fm *fakeManifest) Name() string { return fm.name }

func (fm *fakeManifest) Close() error { return nil }

// ParseIfExists returns any fake manifest data the caller has injected using
// Update() or set(). It treats an empty |fm.lock| as a non-existent manifest.
func (fm *fakeManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if !fm.contents.lock.IsEmpty() {
		return true, fm.contents, nil
	}

	return false, manifestContents{}, nil
}

// Update checks whether |lastLock| == |fm.lock| and, if so, updates internal
// fake manifest state as per the manifest.Update() contract: |fm.lock| is set
// to |newLock|, |fm.root| is set to |newRoot|, and the contents of |specs|
// replace |fm.tableSpecs|. If |lastLock| != |fm.lock|, then the update
// fails. Regardless of success or failure, the current state is returned.
func (fm *fakeManifest) Update(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.contents.lock == lastLock {
		fm.contents = manifestContents{
			manifestVers: StorageVersion,
			nbfVers:      newContents.nbfVers,
			lock:         newContents.lock,
			root:         newContents.root,
			gcGen:        hash.Hash{},
		}
		fm.contents.specs = make([]tableSpec, len(newContents.specs))
		copy(fm.contents.specs, newContents.specs)
		if newContents.appendix != nil && len(newContents.appendix) > 0 {
			fm.contents.appendix = make([]tableSpec, len(newContents.appendix))
			copy(fm.contents.appendix, newContents.appendix)
		}
	}
	return fm.contents, nil
}

// UpdateGCGen mirrors Update, but carries the new gcGen through rather than
// clearing it. Like the real implementations, it requires |lastLock| to match.
func (fm *fakeManifest) UpdateGCGen(ctx context.Context, behavior dherrors.FatalBehavior, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.contents.lock == lastLock {
		fm.contents = manifestContents{
			manifestVers: StorageVersion,
			nbfVers:      newContents.nbfVers,
			lock:         newContents.lock,
			root:         newContents.root,
			gcGen:        newContents.gcGen,
		}
		fm.contents.specs = make([]tableSpec, len(newContents.specs))
		copy(fm.contents.specs, newContents.specs)
		if len(newContents.appendix) > 0 {
			fm.contents.appendix = make([]tableSpec, len(newContents.appendix))
			copy(fm.contents.appendix, newContents.appendix)
		}
	}
	return fm.contents, nil
}

func (fm *fakeManifest) set(version string, lock hash.Hash, root hash.Hash, specs, appendix []tableSpec) {
	fm.contents = manifestContents{
		manifestVers: StorageVersion,
		nbfVers:      version,
		lock:         lock,
		root:         root,
		gcGen:        hash.Hash{},
		specs:        specs,
		appendix:     appendix,
	}
}

func newFakeTableSet(q MemoryQuotaProvider) *tableSet {
	var cnt int32 = 1
	return &tableSet{p: newFakeTablePersister(q), q: q, rl: make(chan struct{}, 1), cnt: &cnt, closeCh: make(chan struct{})}
}

func newFakeTablePersister(q MemoryQuotaProvider) fakeTablePersister {
	return fakeTablePersister{q, map[hash.Hash][]byte{}, map[hash.Hash]bool{}, map[hash.Hash]bool{}, &sync.RWMutex{}}
}

type fakeTablePersister struct {
	q             MemoryQuotaProvider
	sources       map[hash.Hash][]byte
	sourcesToFail map[hash.Hash]bool
	opened        map[hash.Hash]bool
	mu            *sync.RWMutex
}

var _ tablePersister = fakeTablePersister{}

func (ftp fakeTablePersister) Persist(ctx context.Context, behavior dherrors.FatalBehavior, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	if mt.count() == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}

	name, data, _, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	} else if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	} else if chunkCount == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}

	ftp.mu.Lock()
	ftp.sources[name] = data
	ftp.mu.Unlock()

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, gcBehavior_Continue, err
	}

	cs, err := newTableReader(ctx, ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	return chunkSourceAdapter{cs, name}, gcBehavior_Continue, nil
}

func (ftp fakeTablePersister) CopyTableFile(ctx context.Context, r io.Reader, fileId string, fileSz uint64, splitOffset uint64) (io.Closer, error) {
	return nil, errors.New("unimplemented fakeTablePersister.CopyTableFile")
}

func (ftp fakeTablePersister) Path() string {
	return "//fakeTablePersister/"
}

func (ftp fakeTablePersister) ConjoinAll(ctx context.Context, _ dherrors.FatalBehavior, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	name, data, chunkCount, err := compactSourcesToBuffer(sources)
	if err != nil {
		return nil, nil, err
	} else if chunkCount == 0 {
		return emptyChunkSource{}, func() {}, nil
	}

	ftp.mu.Lock()
	defer ftp.mu.Unlock()
	ftp.sources[name] = data

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, nil, err
	}

	cs, err := newTableReader(ctx, ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return nil, nil, err
	}
	return chunkSourceAdapter{cs, name}, func() {}, nil
}

func compactSourcesToBuffer(sources chunkSources) (name hash.Hash, data []byte, chunkCount uint32, err error) {
	totalData := uint64(0)
	for _, src := range sources {
		chunkCount += src.count()
		totalData += mustUint64(src.uncompressedLen())
	}
	if chunkCount == 0 {
		return
	}

	maxSize := maxTableSize(uint64(chunkCount), totalData)
	buff := make([]byte, maxSize) // This can blow up RAM
	tw := newTableWriter(buff, nil)
	errString := ""

	ctx := context.Background()
	for _, src := range sources {
		ch := make(chan extractRecord)
		go func() {
			defer close(ch)
			err = extractAllChunks(ctx, src, func(rec extractRecord) {
				ch <- rec
			})
			if err != nil {
				ch <- extractRecord{a: src.hash(), err: err}
			}
		}()

		for rec := range ch {
			if rec.err != nil {
				errString += fmt.Sprintf("Failed to extract %s:\n %v\n******\n\n", rec.a, rec.err)
				continue
			}
			tw.addChunk(rec.a, rec.data)
		}
	}

	if errString != "" {
		return hash.Hash{}, nil, 0, fmt.Errorf("%s", errString)
	}

	tableSize, name, err := tw.finish()

	if err != nil {
		return hash.Hash{}, nil, 0, err
	}

	return name, buff[:tableSize], chunkCount, nil
}

func (ftp fakeTablePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	ftp.mu.Lock()
	defer ftp.mu.Unlock()

	if _, ok := ftp.sourcesToFail[name]; ok {
		return nil, errors.New("intentional failure")
	}
	data := ftp.sources[name]
	ftp.opened[name] = true

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, err
	}

	cs, err := newTableReader(ctx, ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return emptyChunkSource{}, err
	}
	return chunkSourceAdapter{cs, name}, nil
}

func (ftp fakeTablePersister) Exists(ctx context.Context, name string, chunkCount uint32, stats *Stats) (bool, io.Closer, error) {
	h, ok := hash.MaybeParse(name)
	if !ok {
		panic("object store name expected to be a hash in test")
	}

	if _, ok := ftp.sourcesToFail[h]; ok {
		return false, nil, errors.New("intentional failure")
	}
	return true, noopPendingHandle{}, nil
}

func (ftp fakeTablePersister) PruneTableFiles(_ context.Context) error {
	return chunks.ErrUnsupportedOperation
}

func (ftp fakeTablePersister) Close() error {
	return nil
}

func (ftp fakeTablePersister) Teardown(ctx context.Context) error {
	return nil
}

func (ftp fakeTablePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func extractAllChunks(ctx context.Context, src chunkSource, cb func(rec extractRecord)) (err error) {
	err = src.iterateAllChunks(ctx, func(chunk chunks.Chunk) {
		cb(extractRecord{a: chunk.Hash(), data: chunk.Data()})
	}, &Stats{})
	return
}
