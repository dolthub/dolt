package chunks

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/ref"
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

	// Re-writing the same data should be idempotent and should not result in a second put
	c = NewChunk([]byte(input))
	suite.Store.Put(c)
	suite.Equal(ref, c.Ref())
	assertInputInStore(input, ref, suite.Store, suite.Assert())

	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}
}

func (suite *ChunkStoreTestSuite) TestChunkStoreWriteAfterCloseFails() {
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *ChunkStoreTestSuite) TestChunkStoreWriteAfterChunkFails() {
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	_ = w.Chunk()
	suite.NoError(err)
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *ChunkStoreTestSuite) TestChunkStoreChunkCloses() {
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	w.Chunk()
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	oldRoot := suite.Store.Root()
	suite.Equal(oldRoot, ref.Ref{})

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
