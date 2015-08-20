package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestLevelDBStoreTestSuite(t *testing.T) {
	suite.Run(t, &LevelDBStoreTestSuite{})
}

type LevelDBStoreTestSuite struct {
	ChunkStoreTestSuite
	dir string
}

func (suite *LevelDBStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	store := NewLevelDBStore(suite.dir)
	suite.putCountFn = func() int {
		return store.putCount
	}

	suite.store = store
}

func (suite *LevelDBStoreTestSuite) TearDownTest() {
	os.Remove(suite.dir)
}
