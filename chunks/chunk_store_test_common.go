package chunks

import (
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/suite"
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
	ref := c.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	suite.Store.UpdateRoot(ref, suite.Store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input, ref, suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = NewChunk([]byte(input))
	suite.Store.Put(c)
	suite.Equal(ref, c.Ref())
	assertInputInStore(input, ref, suite.Store, suite.Assert())
	suite.Store.UpdateRoot(ref, suite.Store.Root()) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	oldRoot := suite.Store.Root()
	suite.True(oldRoot.IsEmpty())

	bogusRoot := ref.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	newRoot := ref.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd")   // sha1("Hello, World")

	// Try to update root with bogus oldRoot
	result := suite.Store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = suite.Store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStoreTestSuite) TestChunkStoreGetNonExisting() {
	ref := ref.Parse("sha1-1111111111111111111111111111111111111111")
	c := suite.Store.Get(ref)
	suite.True(c.IsEmpty())
}
