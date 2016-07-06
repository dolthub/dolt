// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
	"testing"

	"github.com/attic-labs/testify/suite"
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
	ms := NewMemoryStore()
	suite.Panics(func() {
		Deserialize(bytes.NewReader(bad), ms, nil)
	})
}
