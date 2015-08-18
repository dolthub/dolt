package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/suite"
)

func TestLevelDBStoreTestSuite(t *testing.T) {
	suite.Run(t, &LevelDBStoreTestSuite{})
}

type LevelDBStoreTestSuite struct {
	suite.Suite
	dir   string
	store LevelDBStore
}

func (suite *LevelDBStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.store = NewLevelDBStore(suite.dir)
}

func (suite *LevelDBStoreTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStorePut() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	// And reading it via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStoreWriteAfterCloseFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStoreWriteAfterRefFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	w.Ref()
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStorePutWithRefAfterClose() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	ref := w.Ref() // Ref() after Close() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStorePutWithMultipleRef() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	w.Ref()
	ref := w.Ref() // Multiple calls to Ref() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *LevelDBStoreTestSuite) TestLevelDBStoreRoot() {
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

func (suite *LevelDBStoreTestSuite) TestLevelDBStoreGetNonExisting() {
	ref := ref.Parse("sha1-1111111111111111111111111111111111111111")
	r := suite.store.Get(ref)
	suite.Nil(r)
}
