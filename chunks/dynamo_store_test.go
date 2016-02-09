package chunks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestDynamoStoreTestSuite(t *testing.T) {
	suite.Run(t, &DynamoStoreTestSuite{})
}

type DynamoStoreTestSuite struct {
	ChunkStoreTestSuite
}

func (suite *DynamoStoreTestSuite) SetupTest() {
	ddb := createFakeDDB(suite.Assert())
	suite.Store = newDynamoStoreFromDDBsvc("table", "namespace", ddb)
	suite.putCountFn = func() int {
		return ddb.numPuts
	}
}

func (suite *DynamoStoreTestSuite) TearDownTest() {
	suite.Store.Close()
}

func TestGetRetrying(t *testing.T) {
	assert := assert.New(t)
	store := newDynamoStoreFromDDBsvc("table", "namespace", createLowCapFakeDDB(assert))

	c1 := NewChunk([]byte("abc"))

	store.Put(c1)
	store.UpdateRoot(c1.Ref(), store.Root()) // Commit writes
	assert.True(store.Has(c1.Ref()))
	store.Close()
}
