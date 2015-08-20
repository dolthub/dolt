package chunks

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestMemoryStoreTestSuite(t *testing.T) {
	suite.Run(t, &MemoryStoreTestSuite{})
}

type MemoryStoreTestSuite struct {
	ChunkStoreTestSuite
}

func (suite *MemoryStoreTestSuite) SetupTest() {
	suite.store = &MemoryStore{}
}

func (suite *MemoryStoreTestSuite) TearDownTest() {
}
