// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
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
	suite.Store = newDynamoStoreFromDDBsvc("table", "namespace", suite.ddb, false)
	suite.putCountFn = func() int {
		return suite.ddb.numPuts
	}
}

func (suite *DynamoStoreTestSuite) TearDownTest() {
	suite.Store.Close()
}

func TestGetRetrying(t *testing.T) {
	assert := assert.New(t)
	store := newDynamoStoreFromDDBsvc("table", "namespace", createLowCapFakeDDB(assert), false)

	c1 := NewChunk([]byte("abc"))

	store.Put(c1)
	store.UpdateRoot(c1.Hash(), store.Root()) // Commit writes
	assert.True(store.Has(c1.Hash()))
	store.Close()
}

func (suite *DynamoStoreTestSuite) TestChunkCompression() {
	c1 := NewChunk(make([]byte, dynamoWriteUnitSize+1))
	suite.Store.Put(c1)
	suite.Store.UpdateRoot(c1.Hash(), suite.Store.Root()) // Commit writes
	suite.True(suite.Store.Has(c1.Hash()))
	suite.Equal(1, suite.ddb.numCompPuts)

	roundTrip := suite.Store.Get(c1.Hash())
	suite.Equal(c1.Data(), roundTrip.Data())
}
