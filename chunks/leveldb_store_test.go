package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
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
	store := NewLevelDBStore(suite.dir, 0)
	suite.putCountFn = func() int {
		return store.putCount
	}

	suite.store = store
}

func (suite *LevelDBStoreTestSuite) TearDownTest() {
	suite.store.Close()
	os.Remove(suite.dir)
}
