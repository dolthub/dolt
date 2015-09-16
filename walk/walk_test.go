package walk

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func TestWalkTestSuite(t *testing.T) {
	suite.Run(t, &WalkAllTestSuite{})
	suite.Run(t, &WalkTestSuite{})
}

type WalkAllTestSuite struct {
	suite.Suite
	cs *chunks.TestStore
}

func (suite *WalkAllTestSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
}

func (suite *WalkAllTestSuite) walkWorker(r ref.Ref, expected int) {
	actual := 0
	AllP(r, suite.cs, func(r ref.Ref) {
		actual++
	}, 1)
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) storeAndRef(v types.Value) ref.Ref {
	return types.WriteValue(v, suite.cs)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.walkWorker(suite.storeAndRef(types.Float64(0.0)), 1)
	suite.walkWorker(suite.storeAndRef(types.NewString("hello")), 1)
}

func (suite *WalkAllTestSuite) TestWalkComposites() {
	suite.walkWorker(suite.storeAndRef(types.NewList()), 1)
	suite.walkWorker(suite.storeAndRef(types.NewList(types.Bool(false), types.Int32(8))), 1)
	suite.walkWorker(suite.storeAndRef(types.NewSet()), 1)
	suite.walkWorker(suite.storeAndRef(types.NewSet(types.Bool(false), types.Int32(8))), 1)
	suite.walkWorker(suite.storeAndRef(types.NewMap()), 1)
	suite.walkWorker(suite.storeAndRef(types.NewMap(types.Int32(8), types.Bool(true), types.Int32(0), types.Bool(false))), 1)
}

func (suite *WalkAllTestSuite) TestWalkNestedComposites() {
	suite.walkWorker(suite.storeAndRef(types.NewList(types.NewSet(), types.Int32(8))), 2)
	suite.walkWorker(suite.storeAndRef(types.NewSet(types.NewList(), types.NewSet())), 3)
	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	//  "mtlist": []
	//  "set": [5 7 8]
	//  []: "wow"
	// }
	nested := types.NewMap(
		types.NewString("string"), types.NewString("string"),
		types.NewString("list"), types.NewList(types.Bool(false), types.Bool(true)),
		types.NewString("map"), types.NewMap(types.NewString("nested"), types.NewString("string")),
		types.NewString("mtlist"), types.NewList(),
		types.NewString("set"), types.NewSet(types.Int32(5), types.Int32(7), types.Int32(8)),
		types.NewList(), types.NewString("wow"))
	suite.walkWorker(suite.storeAndRef(nested), 6)
}

type WalkTestSuite struct {
	WalkAllTestSuite
	shouldSee types.Value
	mustSkip  types.List
	deadValue types.Value
}

func (suite *WalkTestSuite) SetupTest() {
	suite.shouldSee = types.NewList(types.NewString("zzz"))
	suite.deadValue = types.UInt64(0xDEADBEEF)
	suite.mustSkip = types.NewList(suite.deadValue)
	suite.cs = chunks.NewTestStore()
}

func (suite *WalkTestSuite) TestStopWalkImmediately() {
	actual := 0
	SomeP(suite.storeAndRef(types.NewList(types.NewSet(), types.NewList())), suite.cs, func(r ref.Ref) bool {
		actual++
		return true
	}, 1)
	suite.Equal(1, actual)
}

func (suite *WalkTestSuite) skipWorker(composite types.Value) (reached []ref.Ref) {
	SomeP(suite.storeAndRef(composite), suite.cs, func(r ref.Ref) bool {
		suite.NotEqual(r, suite.deadValue.Ref(), "Should never have reached %+v", suite.deadValue)
		reached = append(reached, r)
		return r == suite.mustSkip.Ref()
	}, 1)
	return
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) TestSkipListElement() {
	wholeList := types.NewList(suite.mustSkip, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeList)
	for _, v := range []types.Value{wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSee} {
		suite.Contains(reached, v.Ref(), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 4)
}

func (suite *WalkTestSuite) TestSkipSetElement() {
	wholeSet := types.NewSet(suite.mustSkip, suite.shouldSee).Insert(suite.shouldSee)
	reached := suite.skipWorker(wholeSet)
	for _, v := range []types.Value{wholeSet, suite.mustSkip, suite.shouldSee} {
		suite.Contains(reached, v.Ref(), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 3)
}

func (suite *WalkTestSuite) TestSkipMapValue() {
	shouldAlsoSee := types.NewSet(types.NewString("Also good"))
	wholeMap := types.NewMap(suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []types.Value{wholeMap, suite.mustSkip, shouldAlsoSee, suite.shouldSee, suite.shouldSee} {
		suite.Contains(reached, v.Ref(), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 5)
}

func (suite *WalkTestSuite) TestSkipMapKey() {
	wholeMap := types.NewMap(suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []types.Value{wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee} {
		suite.Contains(reached, v.Ref(), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 5)
}
