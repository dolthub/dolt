package chunks

import (
	"github.com/attic-labs/testify/suite"

	"github.com/attic-labs/noms/hash"
)

type ChunkStoreTestSuite struct {
	suite.Suite
	Store      ChunkStore
	putCountFn func() int
}

func (suite *ChunkStoreTestSuite) TestChunkStorePut() {
	input := "abc"
	c := NewChunk([]byte(input))
	suite.Store.Put(c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", h.String())

	suite.Store.UpdateRoot(h, suite.Store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input, h, suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = NewChunk([]byte(input))
	suite.Store.Put(c)
	suite.Equal(h, c.Hash())
	assertInputInStore(input, h, suite.Store, suite.Assert())
	suite.Store.UpdateRoot(h, suite.Store.Root()) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *ChunkStoreTestSuite) TestChunkStorePutMany() {
	input1, input2 := "abc", "def"
	c1, c2 := NewChunk([]byte(input1)), NewChunk([]byte(input2))
	suite.Store.PutMany([]Chunk{c1, c2})

	suite.Store.UpdateRoot(c1.Hash(), suite.Store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.Store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	oldRoot := suite.Store.Root()
	suite.True(oldRoot.IsEmpty())

	bogusRoot := hash.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	newRoot := hash.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd")   // sha1("Hello, World")

	// Try to update root with bogus oldRoot
	result := suite.Store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = suite.Store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStoreTestSuite) TestChunkStoreGetNonExisting() {
	h := hash.Parse("sha1-1111111111111111111111111111111111111111")
	c := suite.Store.Get(h)
	suite.True(c.IsEmpty())
}
