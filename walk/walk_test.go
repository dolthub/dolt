package walk

import (
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestWalkTestSuite(t *testing.T) {
	suite.Run(t, &WalkAllTestSuite{})
	suite.Run(t, &WalkTestSuite{})
}

type WalkAllTestSuite struct {
	suite.Suite
}

func (suite *WalkAllTestSuite) walkWorker(value types.Value, expected int) {
	actual := 0
	WalkAll(value, func(v types.Value) {
		actual++
	})
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.walkWorker(nil, 1)
	suite.walkWorker(types.Float64(0.0), 1)
	suite.walkWorker(types.NewString("hello"), 1)
}

func (suite *WalkAllTestSuite) TestWalkComposites() {
	suite.walkWorker(types.NewList(), 1)
	suite.walkWorker(types.NewList(types.Bool(true), types.Int32(8)), 3)
	suite.walkWorker(types.NewSet(), 1)
	suite.walkWorker(types.NewSet(types.NewString("A"), types.Int32(8)).Insert(types.Int32(8)), 3)
	suite.walkWorker(types.NewMap(), 1)
	suite.walkWorker(types.NewMap(types.Int32(8), types.Bool(true), types.Int32(0), types.Bool(false)), 5)
}

func (suite *WalkAllTestSuite) TestWalkNestedComposites() {
	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	//  "mtlist": []
	//  "set": [5 7 8]
	// }
	nested := types.NewMap(
		types.NewString("string"), types.NewString("string"),
		types.NewString("list"), types.NewList(types.Bool(false), types.Bool(true)),
		types.NewString("map"), types.NewMap(types.NewString("nested"), types.NewString("string")),
		types.NewString("mtlist"), types.NewList(),
		types.NewString("set"), types.NewSet(types.Int32(5), types.Int32(7), types.Int32(8)))
	suite.walkWorker(nested, 18)
}

type WalkTestSuite struct {
	suite.Suite
	shouldSee types.Value
	mustSkip  types.List
	deadValue types.Value
}

func (suite *WalkTestSuite) SetupTest() {
	suite.shouldSee = types.NewString("zzz")
	suite.deadValue = types.UInt64(0xDEADBEEF)
	suite.mustSkip = types.NewList(suite.deadValue)
}

func (suite *WalkTestSuite) TestStopWalkImmediately() {
	actual := 0
	Walk(types.NewList(types.Bool(true), types.Int16(8)), func(v types.Value) bool {
		actual++
		return true
	})
	suite.Equal(1, actual)
}

func (suite *WalkTestSuite) skipWorker(composite types.Value) types.List {
	reached := types.NewList()
	Walk(composite, func(v types.Value) bool {
		suite.False(v.Equals(suite.deadValue), "Should never have reached %+v", v)
		reached = reached.Append(v)
		return v.Equals(suite.mustSkip)
	})
	return reached
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) TestSkipListElement() {
	wholeList := types.NewList(suite.mustSkip, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeList)
	suite.EqualValues(types.NewList(wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSee), reached)
}

func (suite *WalkTestSuite) TestSkipSetElement() {
	wholeSet := types.NewSet(suite.mustSkip, suite.shouldSee).Insert(suite.shouldSee)
	reached := suite.skipWorker(wholeSet)
	suite.EqualValues(types.NewList(wholeSet, suite.mustSkip, suite.shouldSee), reached)
}

func (suite *WalkTestSuite) TestSkipMapValue() {
	shouldAlsoSee := types.NewString("Also good")
	wholeMap := types.NewMap(suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	suite.EqualValues(types.NewList(wholeMap, shouldAlsoSee, suite.shouldSee, suite.shouldSee, suite.mustSkip), reached)
}

func (suite *WalkTestSuite) TestSkipMapKey() {
	wholeMap := types.NewMap(suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	suite.EqualValues(types.NewList(wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee), reached)
}
