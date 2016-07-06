// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
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
	factory Factory
	dir     string
}

func (suite *LevelDBStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.factory = NewLevelDBStoreFactory(suite.dir, 24, false)
	store := suite.factory.CreateStore("name").(*LevelDBStore)
	suite.putCountFn = func() int {
		return int(store.putCount)
	}

	suite.Store = store
}

func (suite *LevelDBStoreTestSuite) TearDownTest() {
	suite.Store.Close()
	suite.factory.Shutter()
	os.Remove(suite.dir)
}

func (suite *LevelDBStoreTestSuite) TestReservedKeys() {
	// Apparently, the following:
	//  s := []byte("")
	//  s = append(s, 1, 2, 3)
	//  f := append(s, 10, 20, 30)
	//  g := append(s, 4, 5, 6)
	//
	// Results in both f and g being [1, 2, 3, 4, 5, 6]
	// This was happening to us here, so ldb.chunkPrefix was "/chunk/" and ldb.rootKey was "/chun" instead of "/root"
	ldb := suite.factory.CreateStore("").(*LevelDBStore)
	suite.True(bytes.HasSuffix(ldb.rootKey, []byte(rootKeyConst)))
	suite.True(bytes.HasSuffix(ldb.versionKey, []byte(versionKeyConst)))
	suite.True(bytes.HasSuffix(ldb.chunkPrefix, []byte(chunkPrefixConst)))
}
