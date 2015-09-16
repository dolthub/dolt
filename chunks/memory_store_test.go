package chunks

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
)

func TestMemoryStoreTestSuite(t *testing.T) {
	suite.Run(t, &MemoryStoreTestSuite{})
}

type MemoryStoreTestSuite struct {
	ChunkStoreTestSuite
}

func (suite *MemoryStoreTestSuite) SetupTest() {
	suite.Store = NewMemoryStore()
}

func (suite *MemoryStoreTestSuite) TearDownTest() {
	suite.Store.Close()
}

func (suite *MemoryStoreTestSuite) TestBadSerialization() {
	bad := []byte{0, 1} // Not enough bytes to read first length
	chunks, err := Deserialize(bytes.NewReader(bad))
	for _ = range chunks {
	}
	d.Chk.NoChannelError(err)
}
