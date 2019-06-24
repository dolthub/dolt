// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testMemTableSize = 1 << 8

func TestBlockStoreSuite(t *testing.T) {
	suite.Run(t, &BlockStoreSuite{})
}

type BlockStoreSuite struct {
	suite.Suite
	dir        string
	store      *NomsBlockStore
	putCountFn func() int
}

func (suite *BlockStoreSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir("", "")
	suite.NoError(err)
	suite.store = NewLocalStore(context.Background(), suite.dir, testMemTableSize)
	suite.putCountFn = func() int {
		return int(suite.store.putCount)
	}
}

func (suite *BlockStoreSuite) TearDownTest() {
	suite.store.Close()
	os.RemoveAll(suite.dir)
}

func (suite *BlockStoreSuite) TestChunkStoreMissingDir() {
	newDir := filepath.Join(suite.dir, "does-not-exist")
	suite.Panics(func() { NewLocalStore(context.Background(), newDir, testMemTableSize) })
}

func (suite *BlockStoreSuite) TestChunkStoreNotDir() {
	existingFile := filepath.Join(suite.dir, "path-exists-but-is-a-file")
	os.Create(existingFile)
	suite.Panics(func() { NewLocalStore(context.Background(), existingFile, testMemTableSize) })
}

func (suite *BlockStoreSuite) TestChunkStorePut() {
	input := []byte("abc")
	c := chunks.NewChunk(input)
	suite.store.Put(context.Background(), c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	suite.store.Commit(context.Background(), h, suite.store.Root(context.Background())) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input, h, suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = chunks.NewChunk(input)
	suite.store.Put(context.Background(), c)
	suite.Equal(h, c.Hash())
	assertInputInStore(input, h, suite.store, suite.Assert())
	suite.store.Commit(context.Background(), h, suite.store.Root(context.Background())) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *BlockStoreSuite) TestChunkStorePutMany() {
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	suite.store.Put(context.Background(), c1)
	suite.store.Put(context.Background(), c2)

	suite.store.Commit(context.Background(), c1.Hash(), suite.store.Root(context.Background())) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *BlockStoreSuite) TestChunkStoreStatsSummary() {
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	suite.store.Put(context.Background(), c1)
	suite.store.Put(context.Background(), c2)

	suite.store.Commit(context.Background(), c1.Hash(), suite.store.Root(context.Background())) // Commit writes

	summary := suite.store.StatsSummary()
	suite.Contains(summary, c1.Hash().String())
	suite.NotEqual("Unsupported", summary)
}

func (suite *BlockStoreSuite) TestChunkStorePutMoreThanMemTable() {
	input1, input2 := make([]byte, testMemTableSize/2+1), make([]byte, testMemTableSize/2+1)
	rand.Read(input1)
	rand.Read(input2)
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	suite.store.Put(context.Background(), c1)
	suite.store.Put(context.Background(), c2)

	suite.store.Commit(context.Background(), c1.Hash(), suite.store.Root(context.Background())) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
	suite.Len(suite.store.tables.ToSpecs(), 2)
}

func (suite *BlockStoreSuite) TestChunkStoreGetMany() {
	inputs := [][]byte{make([]byte, testMemTableSize/2+1), make([]byte, testMemTableSize/2+1), []byte("abc")}
	rand.Read(inputs[0])
	rand.Read(inputs[1])
	chnx := make([]chunks.Chunk, len(inputs))
	for i, data := range inputs {
		chnx[i] = chunks.NewChunk(data)
		suite.store.Put(context.Background(), chnx[i])
	}
	suite.store.Commit(context.Background(), chnx[0].Hash(), suite.store.Root(context.Background())) // Commit writes

	hashes := make(hash.HashSlice, len(chnx))
	for i, c := range chnx {
		hashes[i] = c.Hash()
	}

	chunkChan := make(chan *chunks.Chunk, len(hashes))
	suite.store.GetMany(context.Background(), hashes.HashSet(), chunkChan)
	close(chunkChan)

	found := make(hash.HashSlice, 0)
	for c := range chunkChan {
		found = append(found, c.Hash())
	}

	sort.Sort(found)
	sort.Sort(hashes)
	suite.True(found.Equals(hashes))
}

func (suite *BlockStoreSuite) TestChunkStoreHasMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.store.Put(context.Background(), c)
	}
	suite.store.Commit(context.Background(), chnx[0].Hash(), suite.store.Root(context.Background())) // Commit writes
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	absent := suite.store.HasMany(context.Background(), hashes)

	suite.Len(absent, 1)
	for _, c := range chnx {
		suite.False(absent.Has(c.Hash()), "%s present in %v", c.Hash(), absent)
	}
	suite.True(absent.Has(notPresent))
}

func (suite *BlockStoreSuite) TestChunkStoreExtractChunks() {
	input1, input2 := make([]byte, testMemTableSize/2+1), make([]byte, testMemTableSize/2+1)
	rand.Read(input1)
	rand.Read(input2)
	chnx := []chunks.Chunk{chunks.NewChunk(input1), chunks.NewChunk(input2)}
	for _, c := range chnx {
		suite.store.Put(context.Background(), c)
	}

	chunkChan := make(chan *chunks.Chunk)
	go func() { suite.store.extractChunks(context.Background(), chunkChan); close(chunkChan) }()
	i := 0
	for c := range chunkChan {
		suite.Equal(chnx[i].Data(), c.Data())
		suite.Equal(chnx[i].Hash(), c.Hash())
		i++
	}
}

func (suite *BlockStoreSuite) TestChunkStoreFlushOptimisticLockFail() {
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	root := suite.store.Root(context.Background())

	interloper := NewLocalStore(context.Background(), suite.dir, testMemTableSize)
	interloper.Put(context.Background(), c1)
	suite.True(interloper.Commit(context.Background(), interloper.Root(context.Background()), interloper.Root(context.Background())))

	suite.store.Put(context.Background(), c2)
	suite.True(suite.store.Commit(context.Background(), suite.store.Root(context.Background()), suite.store.Root(context.Background())))

	// Reading c2 via the API should work...
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// And so should reading c1 via the API
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())

	suite.True(interloper.Commit(context.Background(), c1.Hash(), interloper.Root(context.Background()))) // Commit root

	// Updating from stale root should fail...
	suite.False(suite.store.Commit(context.Background(), c2.Hash(), root))
	// ...but new root should succeed
	suite.True(suite.store.Commit(context.Background(), c2.Hash(), suite.store.Root(context.Background())))
}

func (suite *BlockStoreSuite) TestChunkStoreRebaseOnNoOpFlush() {
	input1 := []byte("abc")
	c1 := chunks.NewChunk(input1)

	interloper := NewLocalStore(context.Background(), suite.dir, testMemTableSize)
	interloper.Put(context.Background(), c1)
	suite.True(interloper.Commit(context.Background(), c1.Hash(), interloper.Root(context.Background())))

	suite.False(suite.store.Has(context.Background(), c1.Hash()))
	suite.Equal(hash.Hash{}, suite.store.Root(context.Background()))
	// Should Rebase, even though there's no work to do.
	suite.True(suite.store.Commit(context.Background(), suite.store.Root(context.Background()), suite.store.Root(context.Background())))

	// Reading c1 via the API should work
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	suite.True(suite.store.Has(context.Background(), c1.Hash()))
}

func (suite *BlockStoreSuite) TestChunkStorePutWithRebase() {
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	root := suite.store.Root(context.Background())

	interloper := NewLocalStore(context.Background(), suite.dir, testMemTableSize)
	interloper.Put(context.Background(), c1)
	suite.True(interloper.Commit(context.Background(), interloper.Root(context.Background()), interloper.Root(context.Background())))

	suite.store.Put(context.Background(), c2)

	// Reading c2 via the API should work pre-rebase
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// Shouldn't have c1 yet.
	suite.False(suite.store.Has(context.Background(), c1.Hash()))

	suite.store.Rebase(context.Background())

	// Reading c2 via the API should work post-rebase
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// And so should reading c1 via the API
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())

	// Commit interloper root
	suite.True(interloper.Commit(context.Background(), c1.Hash(), interloper.Root(context.Background())))

	// suite.store should still have its initial root
	suite.EqualValues(root, suite.store.Root(context.Background()))
	suite.store.Rebase(context.Background())

	// Rebase grabbed the new root, so updating should now succeed!
	suite.True(suite.store.Commit(context.Background(), c2.Hash(), suite.store.Root(context.Background())))

	// Interloper shouldn't see c2 yet....
	suite.False(interloper.Has(context.Background(), c2.Hash()))
	interloper.Rebase(context.Background())
	// ...but post-rebase it must
	assertInputInStore(input2, c2.Hash(), interloper, suite.Assert())
}

func TestBlockStoreConjoinOnCommit(t *testing.T) {
	stats := &Stats{}
	assertContainAll := func(t *testing.T, store chunks.ChunkStore, srcs ...chunkSource) {
		rdrs := make(chunkReaderGroup, len(srcs))
		for i, src := range srcs {
			rdrs[i] = src
		}
		chunkChan := make(chan extractRecord, rdrs.count())
		rdrs.extract(context.Background(), chunkChan)
		close(chunkChan)

		for rec := range chunkChan {
			assert.True(t, store.Has(context.Background(), hash.Hash(rec.a)))
		}
	}

	makeManifestManager := func(m manifest) manifestManager {
		return manifestManager{m, newManifestCache(0), newManifestLocks()}
	}

	newChunk := chunks.NewChunk([]byte("gnu"))

	t.Run("NoConjoin", func(t *testing.T) {
		mm := makeManifestManager(&fakeManifest{})
		p := newFakeTablePersister()
		c := &fakeConjoiner{}

		smallTableStore := newNomsBlockStore(context.Background(), mm, p, c, testMemTableSize)

		root := smallTableStore.Root(context.Background())
		smallTableStore.Put(context.Background(), newChunk)
		assert.True(t, smallTableStore.Commit(context.Background(), newChunk.Hash(), root))
		assert.True(t, smallTableStore.Has(context.Background(), newChunk.Hash()))
	})

	makeCanned := func(conjoinees, keepers []tableSpec, p tablePersister) cannedConjoin {
		srcs := chunkSources{}
		for _, sp := range conjoinees {
			srcs = append(srcs, p.Open(context.Background(), sp.name, sp.chunkCount, nil))
		}
		conjoined := p.ConjoinAll(context.Background(), srcs, stats)
		cannedSpecs := []tableSpec{{conjoined.hash(), conjoined.count()}}
		return cannedConjoin{true, append(cannedSpecs, keepers...)}
	}

	t.Run("ConjoinSuccess", func(t *testing.T) {
		fm := &fakeManifest{}
		p := newFakeTablePersister()

		srcs := makeTestSrcs([]uint32{1, 1, 3, 7}, p)
		upstream := toSpecs(srcs)
		fm.set(constants.NomsVersion, computeAddr([]byte{0xbe}), hash.Of([]byte{0xef}), upstream)
		c := &fakeConjoiner{
			[]cannedConjoin{makeCanned(upstream[:2], upstream[2:], p)},
		}

		smallTableStore := newNomsBlockStore(context.Background(), makeManifestManager(fm), p, c, testMemTableSize)

		root := smallTableStore.Root(context.Background())
		smallTableStore.Put(context.Background(), newChunk)
		assert.True(t, smallTableStore.Commit(context.Background(), newChunk.Hash(), root))
		assert.True(t, smallTableStore.Has(context.Background(), newChunk.Hash()))
		assertContainAll(t, smallTableStore, srcs...)
	})

	t.Run("ConjoinRetry", func(t *testing.T) {
		fm := &fakeManifest{}
		p := newFakeTablePersister()

		srcs := makeTestSrcs([]uint32{1, 1, 3, 7, 13}, p)
		upstream := toSpecs(srcs)
		fm.set(constants.NomsVersion, computeAddr([]byte{0xbe}), hash.Of([]byte{0xef}), upstream)
		c := &fakeConjoiner{
			[]cannedConjoin{
				makeCanned(upstream[:2], upstream[2:], p),
				makeCanned(upstream[:4], upstream[4:], p),
			},
		}

		smallTableStore := newNomsBlockStore(context.Background(), makeManifestManager(fm), p, c, testMemTableSize)

		root := smallTableStore.Root(context.Background())
		smallTableStore.Put(context.Background(), newChunk)
		assert.True(t, smallTableStore.Commit(context.Background(), newChunk.Hash(), root))
		assert.True(t, smallTableStore.Has(context.Background(), newChunk.Hash()))
		assertContainAll(t, smallTableStore, srcs...)
	})
}

type cannedConjoin struct {
	should bool
	specs  []tableSpec // Must name tables that are already persisted
}

type fakeConjoiner struct {
	canned []cannedConjoin
}

func (fc *fakeConjoiner) ConjoinRequired(ts tableSet) bool {
	if len(fc.canned) == 0 {
		return false
	}
	return fc.canned[0].should
}

func (fc *fakeConjoiner) Conjoin(ctx context.Context, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) manifestContents {
	d.PanicIfTrue(len(fc.canned) == 0)
	canned := fc.canned[0]
	fc.canned = fc.canned[1:]

	newContents := manifestContents{
		vers:  constants.NomsVersion,
		root:  upstream.root,
		specs: canned.specs,
		lock:  generateLockHash(upstream.root, canned.specs),
	}

	var err error
	upstream, err = mm.Update(context.Background(), upstream.lock, newContents, stats, nil)

	// TODO: fix panics
	d.PanicIfError(err)

	d.PanicIfFalse(upstream.lock == newContents.lock)
	return upstream
}

func assertInputInStore(input []byte, h hash.Hash, s chunks.ChunkStore, assert *assert.Assertions) {
	c := s.Get(context.Background(), h)
	assert.False(c.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Zero(bytes.Compare(input, c.Data()), "%s != %s", string(input), string(c.Data()))
}

func (suite *BlockStoreSuite) TestChunkStoreGetNonExisting() {
	h := hash.Parse("11111111111111111111111111111111")
	c := suite.store.Get(context.Background(), h)
	suite.True(c.IsEmpty())
}
