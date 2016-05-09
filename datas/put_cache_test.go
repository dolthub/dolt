package datas

import (
	"bytes"
	"sync"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestLevelDBPutCacheSuite(t *testing.T) {
	suite.Run(t, &LevelDBPutCacheSuite{})
}

type LevelDBPutCacheSuite struct {
	suite.Suite
	cache  *unwrittenPutCache
	values []types.Value
	chnx   map[ref.Ref]chunks.Chunk
}

func (suite *LevelDBPutCacheSuite) SetupTest() {
	suite.cache = newUnwrittenPutCache()
	suite.values = []types.Value{
		types.NewString("abc"),
		types.NewString("def"),
		types.NewString("ghi"),
		types.NewString("jkl"),
		types.NewString("mno"),
	}
	suite.chnx = map[ref.Ref]chunks.Chunk{}
	for _, v := range suite.values {
		suite.chnx[v.Ref()] = types.EncodeValue(v, nil)
	}
}

func (suite *LevelDBPutCacheSuite) TearDownTest() {
	suite.cache.Destroy()
}

func (suite *LevelDBPutCacheSuite) TestAddTwice() {
	chunk := suite.chnx[suite.values[0].Ref()]
	suite.True(suite.cache.Add(chunk))
	suite.False(suite.cache.Add(chunk))
}

func (suite *LevelDBPutCacheSuite) TestAddParallel() {
	hashes := make(chan ref.Ref)
	for _, chunk := range suite.chnx {
		go func(c chunks.Chunk) {
			suite.cache.Add(c)
			hashes <- c.Ref()
		}(chunk)
	}

	for i := 0; i < len(suite.values); i++ {
		hash := <-hashes
		suite.True(suite.cache.Has(hash))
		delete(suite.chnx, hash)
	}
	close(hashes)
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestGetParallel() {
	for _, c := range suite.chnx {
		suite.cache.Add(c)
	}

	chunkChan := make(chan chunks.Chunk)
	for hash := range suite.chnx {
		go func(h ref.Ref) {
			chunkChan <- suite.cache.Get(h)
		}(hash)
	}

	for i := 0; i < len(suite.values); i++ {
		c := <-chunkChan
		delete(suite.chnx, c.Ref())
	}
	close(chunkChan)
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestClearParallel() {
	hashes := ref.RefSlice{}
	for h, c := range suite.chnx {
		hashes = append(hashes, h)
		suite.cache.Add(c)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	clear := func(hs ref.RefSlice) {
		suite.cache.Clear(hs)
		wg.Done()
	}
	keepIdx := 2
	go clear(hashes[:keepIdx])
	go clear(hashes[keepIdx+1:])

	wg.Wait()
	for i, hash := range hashes {
		if i == keepIdx {
			suite.True(suite.cache.Has(hash))
			continue
		}
		suite.False(suite.cache.Has(hash))
	}
}

func (suite *LevelDBPutCacheSuite) TestReaderSubset() {
	orderedHashes := ref.RefSlice{}
	for hash, c := range suite.chnx {
		orderedHashes = append(orderedHashes, hash)
		suite.cache.Add(c)
	}

	// Only iterate over the first 2 elements in the DB
	chunkChan := suite.extractChunks(orderedHashes[0], orderedHashes[1])
	origLen := len(orderedHashes)
	for c := range chunkChan {
		suite.Equal(orderedHashes[0], c.Ref())
		orderedHashes = orderedHashes[1:]
	}
	suite.Len(orderedHashes, origLen-2)
}

func (suite *LevelDBPutCacheSuite) TestReaderSnapshot() {
	hashes := ref.RefSlice{}
	for h, c := range suite.chnx {
		hashes = append(hashes, h)
		suite.cache.Add(c)
	}

	chunkChan := suite.extractChunks(hashes[0], hashes[len(hashes)-1])
	// Clear chunks from suite.cache. Should still be enumerated by reader
	suite.cache.Clear(hashes)

	for c := range chunkChan {
		delete(suite.chnx, c.Ref())
	}
	suite.Len(suite.chnx, 0)
}

func (suite *LevelDBPutCacheSuite) TestExtractChunksOrder() {
	orderedHashes := ref.RefSlice{}
	for hash, c := range suite.chnx {
		orderedHashes = append(orderedHashes, hash)
		suite.cache.Add(c)
	}

	chunkChan := suite.extractChunks(orderedHashes[0], orderedHashes[len(orderedHashes)-1])
	for c := range chunkChan {
		suite.Equal(orderedHashes[0], c.Ref())
		orderedHashes = orderedHashes[1:]
	}
	suite.Len(orderedHashes, 0)
}

func (suite *LevelDBPutCacheSuite) extractChunks(start, end ref.Ref) <-chan chunks.Chunk {
	buf := &bytes.Buffer{}
	err := suite.cache.ExtractChunks(start, end, buf)
	suite.NoError(err)

	chunkChan := make(chan chunks.Chunk)
	go chunks.DeserializeToChan(buf, chunkChan)
	return chunkChan
}
