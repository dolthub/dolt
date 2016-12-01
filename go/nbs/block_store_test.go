// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

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
	suite.store = NewBlockStore(suite.dir, defaultMemTableSize)
	suite.putCountFn = func() int {
		return int(suite.store.putCount)
	}
}

func (suite *BlockStoreSuite) TearDownTest() {
	suite.store.Close()
	os.Remove(suite.dir)
}

func (suite *BlockStoreSuite) TestChunkStorePut() {
	input := "abc"
	c := chunks.NewChunk([]byte(input))
	suite.store.Put(c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	suite.store.UpdateRoot(h, suite.store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input, h, suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = chunks.NewChunk([]byte(input))
	suite.store.Put(c)
	suite.Equal(h, c.Hash())
	assertInputInStore(input, h, suite.store, suite.Assert())
	suite.store.UpdateRoot(h, suite.store.Root()) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *BlockStoreSuite) TestChunkStorePutMany() {
	input1, input2 := "abc", "def"
	c1, c2 := chunks.NewChunk([]byte(input1)), chunks.NewChunk([]byte(input2))
	suite.store.PutMany([]chunks.Chunk{c1, c2})

	suite.store.UpdateRoot(c1.Hash(), suite.store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func assertInputInStore(input string, h hash.Hash, s chunks.ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(h)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func (suite *BlockStoreSuite) TestChunkStoreGetNonExisting() {
	h := hash.Parse("11111111111111111111111111111111")
	c := suite.store.Get(h)
	suite.True(c.IsEmpty())
}
