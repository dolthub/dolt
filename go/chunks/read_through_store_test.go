// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"testing"

	"github.com/attic-labs/testify/suite"
)

func TestReadThroughStoreTestSuite(t *testing.T) {
	suite.Run(t, &ReadThroughStoreTestSuite{})
}

type ReadThroughStoreTestSuite struct {
	ChunkStoreTestSuite
}

func (suite *ReadThroughStoreTestSuite) SetupTest() {
	suite.Store = NewReadThroughStore(NewMemoryStore(), NewTestStore())
}

func (suite *ReadThroughStoreTestSuite) TearDownTest() {
	suite.Store.Close()
}

func (suite *LevelDBStoreTestSuite) TestReadThroughStoreGet() {
	bs := NewTestStore()

	// Prepopulate the backing store with "abc".
	input := "abc"
	c := NewChunk([]byte(input))
	bs.Put(c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	suite.Equal(1, bs.Len())
	suite.Equal(1, bs.Writes)
	suite.Equal(0, bs.Reads)

	cs := NewTestStore()
	rts := NewReadThroughStore(cs, bs)

	// Now read "abc". It is not yet in the cache so we hit the backing store.
	chunk := rts.Get(h)
	suite.Equal(input, string(chunk.Data()))

	suite.Equal(1, bs.Len())
	suite.Equal(1, cs.Len())
	suite.Equal(1, cs.Writes)
	suite.Equal(1, bs.Writes)
	suite.Equal(1, cs.Reads)
	suite.Equal(1, bs.Reads)

	// Reading it again should not hit the backing store.
	chunk = rts.Get(h)
	suite.Equal(input, string(chunk.Data()))

	suite.Equal(1, bs.Len())
	suite.Equal(1, cs.Len())
	suite.Equal(1, cs.Writes)
	suite.Equal(1, bs.Writes)
	suite.Equal(2, cs.Reads)
	suite.Equal(1, bs.Reads)
}

func (suite *LevelDBStoreTestSuite) TestReadThroughStorePut() {
	bs := NewTestStore()
	cs := NewTestStore()
	rts := NewReadThroughStore(cs, bs)

	// Storing "abc" should store it to both backing and caching store.
	input := "abc"
	c := NewChunk([]byte(input))
	rts.Put(c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	assertInputInStore("abc", h, bs, suite.Assert())
	assertInputInStore("abc", h, cs, suite.Assert())
	assertInputInStore("abc", h, rts, suite.Assert())
}
