package types

import "github.com/stretchr/testify/suite"

type collectionTestSuite struct {
	suite.Suite
	col                    Collection
	expectType             *Type
	expectLen              uint64
	expectRef              string
	expectChunkCount       int
	expectPrependChunkDiff int
	expectAppendChunkDiff  int
	validate               validateFn
	prependOne             deltaFn
	appendOne              deltaFn
}

type validateFn func(v2 Collection) bool
type deltaFn func() Collection

func (suite *collectionTestSuite) TestRef() {
	suite.Equal(suite.expectRef, suite.col.Ref().String())
}

func (suite *collectionTestSuite) TestType() {
	suite.True(suite.expectType.Equals(suite.col.Type()))
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
	chunks := suite.col.Chunks()
	suite.Equal(suite.expectChunkCount, len(chunks))
	refType := MakeRefType(suite.expectType)
	for _, r := range chunks {
		suite.True(refType.Equals(r.Type()))
	}
}

func (suite *collectionTestSuite) TestRoundTripAndValidate() {
	vs := NewTestValueStore()
	r := vs.WriteValue(suite.col)
	v2 := vs.ReadValue(r.TargetRef()).(Collection)
	suite.True(v2.Equals(suite.col))
	suite.True(suite.col.Equals(v2))
	suite.True(suite.validate(v2))
}

func (suite *collectionTestSuite) TestPrependChunkDiff() {
	v2 := suite.prependOne()
	suite.Equal(suite.expectPrependChunkDiff, chunkDiffCount(suite.col.Chunks(), v2.Chunks()))
}

func (suite *collectionTestSuite) TestAppendChunkDiff() {
	v2 := suite.appendOne()
	suite.Equal(suite.expectAppendChunkDiff, chunkDiffCount(suite.col.Chunks(), v2.Chunks()))
}

func deriveCollectionHeight(c Collection) uint64 {
	// Note: not using mt.childRef.Height() because the purpose of this method is to be redundant.
	seq := c.sequence()
	if seq.seqLen() == 0 {
		return 0
	}
	item := seq.getItem(0)
	if mt, ok := item.(metaTuple); ok {
		return 1 + deriveCollectionHeight(mt.child)
	}

	return 0
}

func getRefHeightOfCollection(c Collection) uint64 {
	return c.sequence().getItem(0).(metaTuple).childRef.Height()
}
