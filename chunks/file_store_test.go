package chunks

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/suite"
)

func TestFileStoreTestSuite(t *testing.T) {
	suite.Run(t, &FileStoreTestSuite{})
}

type FileStoreTestSuite struct {
	suite.Suite
	dir   string
	store FileStore
}

func (suite *FileStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.store = NewFileStore(suite.dir, "root")
}

func (suite *FileStoreTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}

func (suite *FileStoreTestSuite) TestFileStorePut() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	// There should also be a file there now...
	p := path.Join(suite.dir, "sha1", "a9", "99", ref.String())
	f, err := os.Open(p)
	suite.NoError(err)
	data, err := ioutil.ReadAll(f)
	suite.NoError(err)
	suite.Equal(input, string(data))

	// And reading it via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *FileStoreTestSuite) TestFileStoreWriteAfterCloseFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *FileStoreTestSuite) TestFileStoreWriteAfterRefFails() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	_ = w.Ref()
	suite.NoError(err)
	suite.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func (suite *FileStoreTestSuite) TestFileStorePutWithRefAfterClose() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	suite.NoError(w.Close())
	ref := w.Ref() // Ref() after Close() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *FileStoreTestSuite) TestFileStorePutWithMultipleRef() {
	input := "abc"
	w := suite.store.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)

	w.Ref()
	ref := w.Ref() // Multiple calls to Ref() should work...

	// And reading the data via the API should work...
	assertInputInStore(input, ref, suite.store, suite.Assert())
}

func (suite *FileStoreTestSuite) TestFileStoreRoot() {
	oldRoot := suite.store.Root()
	suite.Equal(oldRoot, ref.Ref{})

	// Root file should be absent
	f, err := os.Open(path.Join(suite.dir, "root"))
	suite.True(os.IsNotExist(err))

	bogusRoot := ref.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	newRoot := ref.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd")   // sha1("Hello, World")

	// Try to update root with bogus oldRoot
	result := suite.store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Root file should now be there, but should be empty
	f, err = os.Open(path.Join(suite.dir, "root"))
	suite.NoError(err)
	input, err := ioutil.ReadAll(f)
	suite.Equal(len(input), 0)

	// Now do a valid root update
	result = suite.store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)

	// Root file should now contain "Hello, World" sha1
	f, err = os.Open(path.Join(suite.dir, "root"))
	suite.NoError(err)
	input, err = ioutil.ReadAll(f)
	suite.NoError(err)
	suite.Equal("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd", string(input))
}

func (suite *FileStoreTestSuite) TestFileStorePutExisting() {
	input := "abc"

	mkdirCount := 0
	suite.store.mkdirAll = func(path string, perm os.FileMode) error {
		mkdirCount++
		return os.MkdirAll(path, perm)
	}

	write := func() {
		w := suite.store.Put()
		_, err := w.Write([]byte(input))
		suite.NoError(err)
		w.Ref()
	}

	write()

	suite.Equal(1, mkdirCount)

	write()

	// Shouldn't have written the second time.
	suite.Equal(1, mkdirCount)
}

func (suite *FileStoreTestSuite) TestFileStoreGetNonExisting() {
	ref := ref.Parse("sha1-1111111111111111111111111111111111111111")
	r := suite.store.Get(ref)
	suite.Nil(r)
}
