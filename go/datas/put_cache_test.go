// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
	"github.com/golang/snappy"
)

func TestLevelDBPutCacheSuite(t *testing.T) {
	suite.Run(t, &LevelDBPutCacheSuite{})
}

type LevelDBPutCacheSuite struct {
	suite.Suite
	cache  *orderedChunkCache
	values []types.Value
	chnx   map[hash.Hash]chunks.Chunk
}

func (suite *LevelDBPutCacheSuite) SetupTest() {
	suite.cache = newOrderedChunkCache()
	suite.values = []types.Value{
		types.String("abc"),
		types.String("def"),
		types.String("ghi"),
		types.String("jkl"),
		types.String("mno"),
	}
	suite.chnx = map[hash.Hash]chunks.Chunk{}
	for _, v := range suite.values {
		suite.chnx[v.Hash()] = types.EncodeValue(v, nil)
	}
}

func (suite *LevelDBPutCacheSuite) TearDownTest() {
	suite.cache.Destroy()
}

func (suite *LevelDBPutCacheSuite) TestAddTwice() {
	chunk := suite.chnx[suite.values[0].Hash()]
	suite.True(suite.cache.Insert(chunk, 1))
	suite.False(suite.cache.Insert(chunk, 1))
}

func (suite *LevelDBPutCacheSuite) TestAddParallel() {
	hashes := make(chan hash.Hash)
	for _, chunk := range suite.chnx {
		go func(c chunks.Chunk) {
			suite.cache.Insert(c, 1)
			hashes <- c.Hash()
		}(chunk)
	}

	for i := 0; i < len(suite.values); i++ {
		hash := <-hashes
		suite.True(suite.cache.has(hash))
		delete(suite.chnx, hash)
	}
	close(hashes)
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestGetParallel() {
	for _, c := range suite.chnx {
		suite.cache.Insert(c, 1)
	}

	chunkChan := make(chan chunks.Chunk)
	for h := range suite.chnx {
		go func(h hash.Hash) {
			chunkChan <- suite.cache.Get(h)
		}(h)
	}

	for i := 0; i < len(suite.values); i++ {
		c := <-chunkChan
		delete(suite.chnx, c.Hash())
	}
	close(chunkChan)
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestClearParallel() {
	keepIdx := 2
	toClear1, toClear2 := hashSet{}, hashSet{}
	for i, v := range suite.values {
		suite.cache.Insert(types.EncodeValue(v, nil), 1)
		if i < keepIdx {
			toClear1.Insert(v.Hash())
		} else if i > keepIdx {
			toClear2.Insert(v.Hash())
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	clear := func(hs hashSet) {
		suite.cache.Clear(hs)
		wg.Done()
	}

	go clear(toClear1)
	go clear(toClear2)

	wg.Wait()
	for i, v := range suite.values {
		if i == keepIdx {
			suite.True(suite.cache.has(v.Hash()))
			continue
		}
		suite.False(suite.cache.has(v.Hash()))
	}
}

func (suite *LevelDBPutCacheSuite) TestReaderSubset() {
	toExtract := hashSet{}
	for hash, c := range suite.chnx {
		if len(toExtract) < 2 {
			toExtract.Insert(hash)
		}
		suite.cache.Insert(c, 1)
	}

	// Only iterate over the first 2 elements in the DB
	chunkChan := suite.extractChunks(toExtract)
	count := 0
	for c := range chunkChan {
		if suite.Contains(toExtract, c.Hash()) {
			count++
		}
	}
	suite.Equal(len(toExtract), count)
}

func (suite *LevelDBPutCacheSuite) TestReaderSnapshot() {
	hashes := hashSet{}
	for h, c := range suite.chnx {
		hashes.Insert(h)
		suite.cache.Insert(c, 1)
	}

	chunkChan := suite.extractChunks(hashes)
	// Clear chunks from suite.cache. Should still be enumerated by reader
	suite.cache.Clear(hashes)

	for c := range chunkChan {
		delete(suite.chnx, c.Hash())
	}
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestExtractChunksOrder() {
	maxHeight := len(suite.chnx)
	orderedHashes := make(hash.HashSlice, maxHeight)
	toExtract := hashSet{}
	heights := rand.Perm(maxHeight)
	for hash, c := range suite.chnx {
		toExtract.Insert(hash)
		orderedHashes[heights[0]] = hash
		suite.cache.Insert(c, uint64(heights[0]))
		heights = heights[1:]
	}

	chunkChan := suite.extractChunks(toExtract)
	for c := range chunkChan {
		suite.Equal(orderedHashes[0], c.Hash())
		orderedHashes = orderedHashes[1:]
	}
	suite.Len(orderedHashes, 0)
}

func (suite *LevelDBPutCacheSuite) extractChunks(hashes hashSet) <-chan *chunks.Chunk {
	buf := &bytes.Buffer{}
	err := suite.cache.ExtractChunks(hashes, buf)
	suite.NoError(err)

	chunkChan := make(chan *chunks.Chunk)
	go chunks.DeserializeToChan(snappy.NewReader(buf), chunkChan)
	return chunkChan
}
