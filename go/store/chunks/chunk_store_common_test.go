// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"context"
	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type ChunkStoreTestSuite struct {
	suite.Suite
	Factory *memoryStoreFactory
}

func (suite *ChunkStoreTestSuite) TestChunkStorePut() {
	store := suite.Factory.CreateStore(context.Background(), "ns")
	input := "abc"
	c := NewChunk([]byte(input))
	err := store.Put(context.Background(), c)
	suite.NoError(err)
	h := c.Hash()

	// Reading it via the API should work.
	assertInputInStore(input, h, store, suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	store := suite.Factory.CreateStore(context.Background(), "ns")
	oldRoot, err := store.Root(context.Background())
	suite.NoError(err)
	suite.True(oldRoot.IsEmpty())

	bogusRoot := hash.Parse("8habda5skfek1265pc5d5l1orptn5dr0")
	newRoot := hash.Parse("8la6qjbh81v85r6q67lqbfrkmpds14lg")

	// Try to update root with bogus oldRoot
	result, err := store.Commit(context.Background(), newRoot, bogusRoot)
	suite.NoError(err)
	suite.False(result)

	// Now do a valid root update
	result, err = store.Commit(context.Background(), newRoot, oldRoot)
	suite.NoError(err)
	suite.True(result)
}

func (suite *ChunkStoreTestSuite) TestChunkStoreCommitPut() {
	name := "ns"
	store := suite.Factory.CreateStore(context.Background(), name)
	input := "abc"
	c := NewChunk([]byte(input))
	err := store.Put(context.Background(), c)
	suite.NoError(err)
	h := c.Hash()

	// Reading it via the API should work...
	assertInputInStore(input, h, store, suite.Assert())
	// ...but it shouldn't be persisted yet
	assertInputNotInStore(input, h, suite.Factory.CreateStore(context.Background(), name), suite.Assert())

	r, err := store.Root(context.Background())
	suite.NoError(err)
	_, err = store.Commit(context.Background(), h, r) // Commit persists Chunks
	suite.NoError(err)
	assertInputInStore(input, h, store, suite.Assert())
	assertInputInStore(input, h, suite.Factory.CreateStore(context.Background(), name), suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreGetNonExisting() {
	store := suite.Factory.CreateStore(context.Background(), "ns")
	h := hash.Parse("11111111111111111111111111111111")
	c, err := store.Get(context.Background(), h)
	suite.NoError(err)
	suite.True(c.IsEmpty())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreVersion() {
	store := suite.Factory.CreateStore(context.Background(), "ns")
	oldRoot, err := store.Root(context.Background())
	suite.NoError(err)
	suite.True(oldRoot.IsEmpty())
	newRoot := hash.Parse("11111222223333344444555556666677")
	success, err := store.Commit(context.Background(), newRoot, oldRoot)
	suite.NoError(err)
	suite.True(success)

	suite.Equal(constants.NomsVersion, store.Version())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreCommitUnchangedRoot() {
	store1, store2 := suite.Factory.CreateStore(context.Background(), "ns"), suite.Factory.CreateStore(context.Background(), "ns")
	input := "abc"
	c := NewChunk([]byte(input))
	err := store1.Put(context.Background(), c)
	suite.NoError(err)
	h := c.Hash()

	// Reading c from store1 via the API should work...
	assertInputInStore(input, h, store1, suite.Assert())
	// ...but not store2.
	assertInputNotInStore(input, h, store2, suite.Assert())

	newRoot, err := store1.Root(context.Background())
	suite.NoError(err)
	oldRoot, err := store1.Root(context.Background())
	suite.NoError(err)
	_, err = store1.Commit(context.Background(), newRoot, oldRoot)
	suite.NoError(err)

	err = store2.Rebase(context.Background())
	suite.NoError(err)

	// Now, reading c from store2 via the API should work...
	assertInputInStore(input, h, store2, suite.Assert())
}
