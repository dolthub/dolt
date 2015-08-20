package chunks

import (
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/suite"
)

type ChunkStoreTestSuite struct {
	suite.Suite
	store      ChunkStore
	putCountFn func() int
}

func (suite *ChunkStoreTestSuite) TestChunkStorePut() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	// And reading it via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should be idempotent and should not result in a second put
	w = suite.store.Put()
	_, err = w.Write([]byte(input))
	suite.NoError(err)
	suite.Equal(ref, w.Ref())
	assertInputInStore(input, ref, suite.store, suite.Assert())

	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}
}

func (suite *ChunkStoreTestSuite) TestChunkStoreWriteAfterCloseFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *ChunkStoreTestSuite) TestChunkStoreWriteAfterRefFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	_ = w.Ref()
	suite.NoError(err)
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *ChunkStoreTestSuite) TestChunkStorePutWithRefAfterClose() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	ref := w.Ref() // Ref() after Close() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStorePutWithMultipleRef() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	w.Ref()
	ref := w.Ref() // Multiple calls to Ref() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	oldRoot := suite.store.Root()
	suite.Equal(oldRoot, ref.Ref{})

	bogusRoot := ref.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	newRoot := ref.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd")   // sha1("Hello, World")

	// Try to update root with bogus oldRoot
	result := suite.store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = suite.store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStoreTestSuite) TestChunkStoreGetNonExisting() {
	ref := ref.Parse("sha1-1111111111111111111111111111111111111111")
	r := suite.store.Get(ref)
	suite.Nil(r)
}
