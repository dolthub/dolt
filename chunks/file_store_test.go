package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestFileStoreTestSuite(t *testing.T) {
	suite.Run(t, &FileStoreTestSuite{})
}

type FileStoreTestSuite struct {
	ChunkStoreTestSuite
	dir      string
	putCount int
}

func (suite *FileStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)

	store := NewFileStore(suite.dir, "root")
	suite.putCount = 0
	store.mkdirAll = func(path string, perm os.FileMode) error {
		suite.putCount++
		return os.MkdirAll(path, perm)
	}
	suite.putCountFn = func() int {
		return suite.putCount
	}
	suite.store = store
}

func (suite *FileStoreTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}
