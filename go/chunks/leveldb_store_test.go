// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/testify/suite"
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
	store := NewLevelDBStore(suite.dir, "name", 24, false)
	suite.putCountFn = func() int {
		return int(store.putCount)
	}

	suite.Store = store
}

func (suite *LevelDBStoreTestSuite) TearDownTest() {
	suite.Store.Close()
	os.Remove(suite.dir)
}
