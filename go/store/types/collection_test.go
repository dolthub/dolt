// Copyright 2019 Dolthub, Inc.
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

package types

import (
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/d"
)

type collectionTestSuite struct {
	suite.Suite
	col                    Collection
	expectType             *Type
	expectLen              uint64
	expectChunkCount       int
	expectPrependChunkDiff int
	expectAppendChunkDiff  int
	validate               validateFn
	prependOne             deltaFn
	appendOne              deltaFn
}

type validateFn func(v2 Collection) bool
type deltaFn func() (Collection, error)

func (suite *collectionTestSuite) TestType() {
	t, err := TypeOf(suite.col)
	suite.NoError(err)
	suite.True(suite.expectType.Equals(t))
}

func (suite *collectionTestSuite) TestLen() {
	suite.Equal(suite.expectLen, suite.col.Len())
	suite.Equal(suite.col.Empty(), suite.expectLen == 0)
}

func (suite *collectionTestSuite) TestEquals() {
	v2 := suite.col
	suite.True(suite.col.Equals(v2))
	suite.True(v2.Equals(suite.col))
}

func (suite *collectionTestSuite) TestChunkCountAndType() {
	suite.Equal(suite.expectChunkCount, leafCount(suite.col), "chunk count")
	refType, err := MakeRefType(suite.expectType)
	suite.NoError(err)
	err = suite.col.walkRefs(suite.col.asSequence().format(), func(r Ref) error {
		t, err := TypeOf(r)

		if err != nil {
			return err
		}

		suite.True(refType.Equals(t))
		return nil
	})

	suite.NoError(err)
}

func (suite *collectionTestSuite) TestRoundTripAndValidate() {
	suite.True(suite.validate(suite.col))
}

func (suite *collectionTestSuite) TestPrependChunkDiff() {
	v2, err := suite.prependOne()
	suite.NoError(err)
	suite.Equal(suite.expectPrependChunkDiff, leafDiffCount(suite.col, v2), "prepend count")
}

func (suite *collectionTestSuite) TestAppendChunkDiff() {
	v2, err := suite.appendOne()
	suite.NoError(err)
	suite.Equal(suite.expectAppendChunkDiff, leafDiffCount(suite.col, v2), "append count")
}

func deriveCollectionHeight(c Collection) uint64 {
	return c.asSequence().treeLevel()
}

func getRefHeightOfCollection(c Collection) uint64 {
	item, err := c.asSequence().getItem(0)
	d.PanicIfError(err)
	ref, err := item.(metaTuple).ref()
	d.PanicIfError(err)

	return ref.Height()
}
