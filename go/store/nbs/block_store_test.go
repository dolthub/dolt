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
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

const testMemTableSize = 1 << 8

func TestLocalStoreSuite(t *testing.T) {
	fn := func(ctx context.Context, dir string) (*NomsBlockStore, error) {
		nbf := constants.FormatDefaultString
		qp := NewUnlimitedMemQuotaProvider()
		return NewLocalStore(ctx, nbf, dir, testMemTableSize, qp, false)
	}
	suite.Run(t, &BlockStoreSuite{factory: fn})
}

func TestBlobstoreSuite(t *testing.T) {
	fn := func(ctx context.Context, dir string) (*NomsBlockStore, error) {
		nbf := constants.FormatDefaultString
		qp := NewUnlimitedMemQuotaProvider()
		bs := blobstore.NewLocalBlobstore(dir)
		return NewBSStore(ctx, nbf, bs, testMemTableSize, qp)
	}
	suite.Run(t, &BlockStoreSuite{factory: fn})
}

type BlockStoreSuite struct {
	suite.Suite
	dir        string
	store      *NomsBlockStore
	factory    nbsFactory
	putCountFn func() int

	// if true, skip interloper tests
	skipInterloper bool
}

type nbsFactory func(ctx context.Context, dir string) (*NomsBlockStore, error)

func (suite *BlockStoreSuite) SetupTest() {
	var err error
	suite.dir, err = os.MkdirTemp("", "")
	suite.NoError(err)
	ctx := context.Background()
	suite.store, err = suite.factory(ctx, suite.dir)
	suite.NoError(err)
	suite.putCountFn = func() int {
		return int(suite.store.putCount)
	}
}

func (suite *BlockStoreSuite) TearDownTest() {
	err := suite.store.Close()
	if !osutil.IsWindowsSharingViolation(err) {
		suite.NoError(err)
	}
	err = file.RemoveAll(suite.dir)
	if !osutil.IsWindowsSharingViolation(err) {
		suite.NoError(err)
	}
}

func (suite *BlockStoreSuite) TestChunkStoreMissingDir() {
	newDir := filepath.Join(suite.dir, "does-not-exist")
	_, err := NewLocalStore(context.Background(), constants.FormatDefaultString, newDir, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
	suite.Error(err)
}

func (suite *BlockStoreSuite) TestChunkStoreNotDir() {
	existingFile := filepath.Join(suite.dir, "path-exists-but-is-a-file")
	f, err := os.Create(existingFile)
	suite.NoError(err)
	defer f.Close()

	_, err = NewLocalStore(context.Background(), constants.FormatDefaultString, existingFile, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
	suite.Error(err)
}

func noopGetAddrs(c chunks.Chunk) chunks.GetAddrsCb {
	return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
		return nil
	}
}

func (suite *BlockStoreSuite) TestChunkStorePut() {
	input := []byte("abc")
	c := chunks.NewChunk(input)
	err := suite.store.Put(context.Background(), c, noopGetAddrs)
	suite.NoError(err)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	success, err := suite.store.Commit(context.Background(), h, rt) // Commit writes
	suite.NoError(err)
	suite.True(success)

	// And reading it via the API should work...
	assertInputInStore(input, h, suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = chunks.NewChunk(input)
	err = suite.store.Put(context.Background(), c, noopGetAddrs)
	suite.NoError(err)
	suite.Equal(h, c.Hash())
	assertInputInStore(input, h, suite.store, suite.Assert())
	rt, err = suite.store.Root(context.Background())
	suite.NoError(err)
	_, err = suite.store.Commit(context.Background(), h, rt) // Commit writes
	suite.NoError(err)

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}

	// Put chunk with dangling ref should error on Commit
	nc := chunks.NewChunk([]byte("bcd"))
	err = suite.store.Put(context.Background(), nc, func(c chunks.Chunk) chunks.GetAddrsCb {
		return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
			addrs.Insert(hash.Of([]byte("lorem ipsum")))
			return nil
		}
	})
	suite.NoError(err)
	root, err := suite.store.Root(context.Background())
	suite.NoError(err)
	_, err = suite.store.Commit(context.Background(), root, root)
	suite.Error(err)
}

func (suite *BlockStoreSuite) TestChunkStorePutMany() {
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	err := suite.store.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	err = suite.store.Put(context.Background(), c2, noopGetAddrs)
	suite.NoError(err)

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	success, err := suite.store.Commit(context.Background(), c1.Hash(), rt) // Commit writes
	suite.NoError(err)
	suite.True(success)

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
	err := suite.store.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	err = suite.store.Put(context.Background(), c2, noopGetAddrs)
	suite.NoError(err)

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	success, err := suite.store.Commit(context.Background(), c1.Hash(), rt) // Commit writes
	suite.True(success)
	suite.NoError(err)

	summary := suite.store.StatsSummary()
	suite.Contains(summary, c1.Hash().String())
	suite.NotEqual("Unsupported", summary)
}

func (suite *BlockStoreSuite) TestChunkStorePutMoreThanMemTable() {
	input1, input2 := make([]byte, testMemTableSize/2+1), make([]byte, testMemTableSize/2+1)
	_, err := rand.Read(input1)
	suite.NoError(err)
	_, err = rand.Read(input2)
	suite.NoError(err)
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	err = suite.store.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	err = suite.store.Put(context.Background(), c2, noopGetAddrs)
	suite.NoError(err)

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	success, err := suite.store.Commit(context.Background(), c1.Hash(), rt) // Commit writes
	suite.NoError(err)
	suite.True(success)

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
	sz, err := suite.store.tables.physicalLen()
	suite.NoError(err)
	suite.True(sz > testMemTableSize)
}

func (suite *BlockStoreSuite) TestChunkStoreGetMany() {
	inputs := [][]byte{make([]byte, testMemTableSize/2+1), make([]byte, testMemTableSize/2+1), []byte("abc")}
	_, err := rand.Read(inputs[0])
	suite.NoError(err)
	_, err = rand.Read(inputs[1])
	suite.NoError(err)
	chnx := make([]chunks.Chunk, len(inputs))
	for i, data := range inputs {
		chnx[i] = chunks.NewChunk(data)
		err = suite.store.Put(context.Background(), chnx[i], noopGetAddrs)
		suite.NoError(err)
	}

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	_, err = suite.store.Commit(context.Background(), chnx[0].Hash(), rt) // Commit writes
	suite.NoError(err)

	hashes := make(hash.HashSlice, len(chnx))
	for i, c := range chnx {
		hashes[i] = c.Hash()
	}

	chunkChan := make(chan *chunks.Chunk, len(hashes))
	err = suite.store.GetMany(context.Background(), hashes.HashSet(), func(ctx context.Context, c *chunks.Chunk) {
		select {
		case chunkChan <- c:
		case <-ctx.Done():
		}
	})
	suite.NoError(err)
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
		err := suite.store.Put(context.Background(), c, noopGetAddrs)
		suite.NoError(err)
	}

	rt, err := suite.store.Root(context.Background())
	suite.NoError(err)
	success, err := suite.store.Commit(context.Background(), chnx[0].Hash(), rt) // Commit writes
	suite.NoError(err)
	suite.True(success)
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	absent, err := suite.store.HasMany(context.Background(), hashes)
	suite.NoError(err)

	suite.Len(absent, 1)
	for _, c := range chnx {
		suite.False(absent.Has(c.Hash()), "%s present in %v", c.Hash(), absent)
	}
	suite.True(absent.Has(notPresent))
}

func (suite *BlockStoreSuite) TestChunkStoreFlushOptimisticLockFail() {
	if suite.skipInterloper {
		suite.T().Skip()
	}
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	root, err := suite.store.Root(context.Background())
	suite.NoError(err)

	interloper, err := suite.factory(context.Background(), suite.dir)
	suite.NoError(err)
	defer interloper.Close()
	err = interloper.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	h, err := interloper.Root(context.Background())
	suite.NoError(err)
	success, err := interloper.Commit(context.Background(), h, h)
	suite.NoError(err)
	suite.True(success)

	err = suite.store.Put(context.Background(), c2, noopGetAddrs)
	suite.NoError(err)
	h, err = suite.store.Root(context.Background())
	suite.NoError(err)
	success, err = suite.store.Commit(context.Background(), h, h)
	suite.NoError(err)
	suite.True(success)

	// Reading c2 via the API should work...
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// And so should reading c1 via the API
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())

	h, err = interloper.Root(context.Background())
	suite.NoError(err)
	success, err = interloper.Commit(context.Background(), c1.Hash(), h) // Commit root
	suite.NoError(err)
	suite.True(success)

	// Updating from stale root should fail...
	success, err = suite.store.Commit(context.Background(), c2.Hash(), root)
	suite.NoError(err)
	suite.False(success)

	// ...but new root should succeed
	h, err = suite.store.Root(context.Background())
	suite.NoError(err)
	success, err = suite.store.Commit(context.Background(), c2.Hash(), h)
	suite.NoError(err)
	suite.True(success)
}

func (suite *BlockStoreSuite) TestChunkStoreRebaseOnNoOpFlush() {
	if suite.skipInterloper {
		suite.T().Skip()
	}
	input1 := []byte("abc")
	c1 := chunks.NewChunk(input1)

	interloper, err := suite.factory(context.Background(), suite.dir)
	suite.NoError(err)
	defer interloper.Close()
	err = interloper.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	root, err := interloper.Root(context.Background())
	suite.NoError(err)
	success, err := interloper.Commit(context.Background(), c1.Hash(), root)
	suite.NoError(err)
	suite.True(success)

	has, err := suite.store.Has(context.Background(), c1.Hash())
	suite.NoError(err)
	suite.False(has)

	root, err = suite.store.Root(context.Background())
	suite.NoError(err)
	suite.Equal(hash.Hash{}, root)

	// Should Rebase, even though there's no work to do.
	root, err = suite.store.Root(context.Background())
	suite.NoError(err)
	success, err = suite.store.Commit(context.Background(), root, root)
	suite.NoError(err)
	suite.True(success)

	// Reading c1 via the API should work
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	suite.True(suite.store.Has(context.Background(), c1.Hash()))
}

func (suite *BlockStoreSuite) TestChunkStorePutWithRebase() {
	if suite.skipInterloper {
		suite.T().Skip()
	}
	input1, input2 := []byte("abc"), []byte("def")
	c1, c2 := chunks.NewChunk(input1), chunks.NewChunk(input2)
	root, err := suite.store.Root(context.Background())
	suite.NoError(err)

	interloper, err := suite.factory(context.Background(), suite.dir)
	suite.NoError(err)
	defer interloper.Close()
	err = interloper.Put(context.Background(), c1, noopGetAddrs)
	suite.NoError(err)
	h, err := interloper.Root(context.Background())
	suite.NoError(err)
	success, err := interloper.Commit(context.Background(), h, h)
	suite.NoError(err)
	suite.True(success)

	err = suite.store.Put(context.Background(), c2, noopGetAddrs)
	suite.NoError(err)

	// Reading c2 via the API should work pre-rebase
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// Shouldn't have c1 yet.
	suite.False(suite.store.Has(context.Background(), c1.Hash()))

	err = suite.store.Rebase(context.Background())
	suite.NoError(err)

	// Reading c2 via the API should work post-rebase
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	// And so should reading c1 via the API
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())

	// Commit interloper root
	h, err = interloper.Root(context.Background())
	suite.NoError(err)
	success, err = interloper.Commit(context.Background(), c1.Hash(), h)
	suite.NoError(err)
	suite.True(success)

	// suite.store should still have its initial root
	h, err = suite.store.Root(context.Background())
	suite.NoError(err)
	suite.EqualValues(root, h)
	err = suite.store.Rebase(context.Background())
	suite.NoError(err)

	// Rebase grabbed the new root, so updating should now succeed!
	h, err = suite.store.Root(context.Background())
	suite.NoError(err)
	success, err = suite.store.Commit(context.Background(), c2.Hash(), h)
	suite.NoError(err)
	suite.True(success)

	// Interloper shouldn't see c2 yet....
	suite.False(interloper.Has(context.Background(), c2.Hash()))
	err = interloper.Rebase(context.Background())
	suite.NoError(err)
	// ...but post-rebase it must
	assertInputInStore(input2, c2.Hash(), interloper, suite.Assert())
}

func TestBlockStoreConjoinOnCommit(t *testing.T) {
	t.Run("fake table persister", func(t *testing.T) {
		testBlockStoreConjoinOnCommit(t, func(t *testing.T) tablePersister {
			q := NewUnlimitedMemQuotaProvider()
			return newFakeTablePersister(q)
		})
	})
	t.Run("in memory blobstore persister", func(t *testing.T) {
		testBlockStoreConjoinOnCommit(t, func(t *testing.T) tablePersister {
			return &blobstorePersister{
				bs:        blobstore.NewInMemoryBlobstore(""),
				blockSize: 4096,
				q:         &UnlimitedQuotaProvider{},
			}
		})
	})
}

func testBlockStoreConjoinOnCommit(t *testing.T, factory func(t *testing.T) tablePersister) {
	assertContainAll := func(t *testing.T, store chunks.ChunkStore, sources ...chunkSource) {
		ctx := context.Background()
		for _, src := range sources {
			err := extractAllChunks(ctx, src, func(rec extractRecord) {
				ok, err := store.Has(context.Background(), hash.Hash(rec.a))
				require.NoError(t, err)
				assert.True(t, ok, "chunk %s from chunkSource %s not found in store",
					rec.a.String(), src.hash().String())
			})
			require.NoError(t, err)
		}
	}

	makeManifestManager := func(m manifest) manifestManager {
		return manifestManager{m, newManifestCache(0), newManifestLocks()}
	}

	newChunk := chunks.NewChunk([]byte("gnu"))

	t.Run("NoConjoin", func(t *testing.T) {
		mm := makeManifestManager(&fakeManifest{})
		q := NewUnlimitedMemQuotaProvider()
		defer func() {
			require.EqualValues(t, 0, q.Usage())
		}()
		p := factory(t)

		c := &fakeConjoiner{}

		smallTableStore, err := newNomsBlockStore(context.Background(), constants.FormatDefaultString, mm, p, q, c, testMemTableSize)
		require.NoError(t, err)
		defer smallTableStore.Close()

		root, err := smallTableStore.Root(context.Background())
		require.NoError(t, err)
		err = smallTableStore.Put(context.Background(), newChunk, noopGetAddrs)
		require.NoError(t, err)
		success, err := smallTableStore.Commit(context.Background(), newChunk.Hash(), root)
		require.NoError(t, err)
		assert.True(t, success)

		ok, err := smallTableStore.Has(context.Background(), newChunk.Hash())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("ConjoinSuccess", func(t *testing.T) {
		q := NewUnlimitedMemQuotaProvider()
		fm := &fakeManifest{}
		p := factory(t)

		srcs := makeTestSrcs(t, []uint32{1, 1, 3, 7}, p)
		upstream, err := toSpecs(srcs)
		require.NoError(t, err)
		fm.set(constants.FormatLD1String, computeAddr([]byte{0xbe}), hash.Of([]byte{0xef}), upstream, nil)
		c := &fakeConjoiner{
			[]cannedConjoin{
				{conjoinees: upstream[:2], keepers: upstream[2:]},
			},
		}

		smallTableStore, err := newNomsBlockStore(context.Background(), constants.FormatDefaultString, makeManifestManager(fm), p, q, c, testMemTableSize)
		require.NoError(t, err)
		defer smallTableStore.Close()

		root, err := smallTableStore.Root(context.Background())
		require.NoError(t, err)
		err = smallTableStore.Put(context.Background(), newChunk, noopGetAddrs)
		require.NoError(t, err)
		success, err := smallTableStore.Commit(context.Background(), newChunk.Hash(), root)
		require.NoError(t, err)
		assert.True(t, success)
		ok, err := smallTableStore.Has(context.Background(), newChunk.Hash())
		require.NoError(t, err)
		assert.True(t, ok)
		assertContainAll(t, smallTableStore, srcs...)
		for _, src := range srcs {
			err := src.close()
			require.NoError(t, err)
		}
	})

	t.Run("ConjoinRetry", func(t *testing.T) {
		fm := &fakeManifest{}
		q := NewUnlimitedMemQuotaProvider()
		p := factory(t)

		srcs := makeTestSrcs(t, []uint32{1, 1, 3, 7, 13}, p)
		upstream, err := toSpecs(srcs)
		require.NoError(t, err)
		fm.set(constants.FormatLD1String, computeAddr([]byte{0xbe}), hash.Of([]byte{0xef}), upstream, nil)
		c := &fakeConjoiner{
			[]cannedConjoin{
				{conjoinees: upstream[:2], keepers: upstream[2:]},
				{conjoinees: upstream[:4], keepers: upstream[4:]},
			},
		}

		smallTableStore, err := newNomsBlockStore(context.Background(), constants.FormatDefaultString, makeManifestManager(fm), p, q, c, testMemTableSize)
		require.NoError(t, err)
		defer smallTableStore.Close()

		root, err := smallTableStore.Root(context.Background())
		require.NoError(t, err)
		err = smallTableStore.Put(context.Background(), newChunk, noopGetAddrs)
		require.NoError(t, err)
		success, err := smallTableStore.Commit(context.Background(), newChunk.Hash(), root)
		require.NoError(t, err)
		assert.True(t, success)
		ok, err := smallTableStore.Has(context.Background(), newChunk.Hash())
		require.NoError(t, err)
		assert.True(t, ok)
		assertContainAll(t, smallTableStore, srcs...)
		for _, src := range srcs {
			err := src.close()
			require.NoError(t, err)
		}
	})
}

type cannedConjoin struct {
	// Must name tables that are already persisted
	conjoinees, keepers []tableSpec
}

type fakeConjoiner struct {
	canned []cannedConjoin
}

func (fc *fakeConjoiner) conjoinRequired(ts tableSet) bool {
	if len(fc.canned) == 0 {
		return false
	}
	return true
}

func (fc *fakeConjoiner) chooseConjoinees(specs []tableSpec) (conjoinees, keepers []tableSpec, err error) {
	d.PanicIfTrue(len(fc.canned) == 0)
	cur := fc.canned[0]
	fc.canned = fc.canned[1:]
	conjoinees, keepers = cur.conjoinees, cur.keepers
	return
}

func assertInputInStore(input []byte, h hash.Hash, s chunks.ChunkStore, assert *assert.Assertions) {
	ctx := context.Background()
	c, err := s.Get(ctx, h)
	assert.NoError(err)
	if c.IsEmpty() {
		c, err = s.Get(ctx, h)
	}
	assert.False(c.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Zero(bytes.Compare(input, c.Data()), "%s != %s", string(input), string(c.Data()))
}

func (suite *BlockStoreSuite) TestChunkStoreGetNonExisting() {
	h := hash.Parse("11111111111111111111111111111111")
	c, err := suite.store.Get(context.Background(), h)
	suite.NoError(err)
	suite.True(c.IsEmpty())
}
