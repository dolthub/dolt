// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/stretchr/testify/suite"

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
type deltaFn func() Collection

func (suite *collectionTestSuite) TestType() {
	suite.True(suite.expectType.Equals(Format_7_18, TypeOf(suite.col)))
}

func (suite *collectionTestSuite) TestLen() {
	suite.Equal(suite.expectLen, suite.col.Len())
	suite.Equal(suite.col.Empty(), suite.expectLen == 0)
}

func (suite *collectionTestSuite) TestEquals() {
	v2 := suite.col
	suite.True(suite.col.Equals(Format_7_18, v2))
	suite.True(v2.Equals(Format_7_18, suite.col))
}

func (suite *collectionTestSuite) TestChunkCountAndType() {
	suite.Equal(suite.expectChunkCount, leafCount(suite.col), "chunk count")
	refType := MakeRefType(suite.expectType)
	suite.col.WalkRefs(Format_7_18, func(r Ref) {
		suite.True(refType.Equals(Format_7_18, TypeOf(r)))
	})
}

func (suite *collectionTestSuite) TestRoundTripAndValidate() {
	suite.True(suite.validate(suite.col))
}

func (suite *collectionTestSuite) TestPrependChunkDiff() {
	v2 := suite.prependOne()
	suite.Equal(suite.expectPrependChunkDiff, leafDiffCount(suite.col, v2), "prepend count")
}

func (suite *collectionTestSuite) TestAppendChunkDiff() {
	v2 := suite.appendOne()
	suite.Equal(suite.expectAppendChunkDiff, leafDiffCount(suite.col, v2), "append count")
}

func deriveCollectionHeight(c Collection) uint64 {
	return c.asSequence().treeLevel()
}

func getRefHeightOfCollection(c Collection) uint64 {
	// TODO(binformat)
	return c.asSequence().getItem(0, Format_7_18).(metaTuple).ref().Height()
}
