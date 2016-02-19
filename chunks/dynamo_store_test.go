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
	ddb *fakeDDB
}

func (suite *DynamoStoreTestSuite) SetupTest() {
	suite.ddb = createFakeDDB(suite.Assert())
	suite.Store = newDynamoStoreFromDDBsvc("table", "namespace", suite.ddb)
	suite.putCountFn = func() int {
		return suite.ddb.numPuts
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

func (suite *DynamoStoreTestSuite) TestChunkCompression() {
	c1 := NewChunk(make([]byte, dynamoWriteUnitSize+1))
	suite.Store.Put(c1)
	suite.Store.UpdateRoot(c1.Ref(), suite.Store.Root()) // Commit writes
	suite.True(suite.Store.Has(c1.Ref()))
	suite.Equal(1, suite.ddb.numCompPuts)

	roundTrip := suite.Store.Get(c1.Ref())
	suite.Equal(c1.Data(), roundTrip.Data())
}
