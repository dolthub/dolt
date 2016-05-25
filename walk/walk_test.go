package walk

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/suite"
)

func TestWalkTestSuite(t *testing.T) {
	suite.Run(t, &WalkAllTestSuite{})
	suite.Run(t, &WalkTestSuite{})
}

type WalkAllTestSuite struct {
	suite.Suite
	vs *types.ValueStore
}

func (suite *WalkAllTestSuite) SetupTest() {
	suite.vs = types.NewTestValueStore()
}

func (suite *WalkAllTestSuite) walkWorker(r types.Ref, expected int) {
	actual := 0
	AllP(r, suite.vs, func(c types.Value, r *types.Ref) {
		actual++
	}, 1)
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) storeAndRef(v types.Value) types.Ref {
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.walkWorker(suite.storeAndRef(types.Number(0.0)), 2)
	suite.walkWorker(suite.storeAndRef(types.NewString("hello")), 2)
}

func (suite *WalkAllTestSuite) TestWalkComposites() {
	suite.walkWorker(suite.storeAndRef(types.NewList()), 2)
	suite.walkWorker(suite.storeAndRef(types.NewList(types.Bool(false), types.Number(8))), 4)
	suite.walkWorker(suite.storeAndRef(types.NewSet()), 2)
	suite.walkWorker(suite.storeAndRef(types.NewSet(types.Bool(false), types.Number(8))), 4)
	suite.walkWorker(suite.storeAndRef(types.NewMap()), 2)
	suite.walkWorker(suite.storeAndRef(types.NewMap(types.Number(8), types.Bool(true), types.Number(0), types.Bool(false))), 6)
}

func (suite *WalkAllTestSuite) NewList(cs chunks.ChunkStore, vs ...types.Value) types.Ref {
	v := types.NewList(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) NewMap(cs chunks.ChunkStore, vs ...types.Value) types.Ref {
	v := types.NewMap(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) NewSet(cs chunks.ChunkStore, vs ...types.Value) types.Ref {
	v := types.NewSet(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) TestWalkNestedComposites() {
	cs := chunks.NewMemoryStore()
	suite.walkWorker(suite.storeAndRef(types.NewList(suite.NewSet(cs), types.Number(8))), 5)
	suite.walkWorker(suite.storeAndRef(types.NewSet(suite.NewList(cs), suite.NewSet(cs))), 6)
	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	//  "mtlist": []
	//  "set": [5 7 8]
	//  []: "wow"
	// }
	nested := types.NewMap(
		types.NewString("string"), types.NewString("string"),
		types.NewString("list"), suite.NewList(cs, types.Bool(false), types.Bool(true)),
		types.NewString("map"), suite.NewMap(cs, types.NewString("nested"), types.NewString("string")),
		types.NewString("mtlist"), suite.NewList(cs),
		types.NewString("set"), suite.NewSet(cs, types.Number(5), types.Number(7), types.Number(8)),
		suite.NewList(cs), types.NewString("wow"), // note that the dupe list chunk is skipped
	)
	suite.walkWorker(suite.storeAndRef(nested), 25)
}

type WalkTestSuite struct {
	WalkAllTestSuite
	shouldSeeItem types.Value
	shouldSee     types.Value
	mustSkip      types.Value
	deadValue     types.Value
}

func (suite *WalkTestSuite) SetupTest() {
	suite.vs = types.NewTestValueStore()
	suite.shouldSeeItem = types.NewString("zzz")
	suite.shouldSee = types.NewList(suite.shouldSeeItem)
	suite.deadValue = types.Number(0xDEADBEEF)
	suite.mustSkip = types.NewList(suite.deadValue)
}

func (suite *WalkTestSuite) TestStopWalkImmediately() {
	actual := 0
	SomeP(types.NewList(types.NewSet(), types.NewList()), suite.vs, func(v types.Value, r *types.Ref) bool {
		actual++
		return true
	}, 1)
	suite.Equal(1, actual)
}

func (suite *WalkTestSuite) skipWorker(composite types.Value) (reached []types.Value) {
	SomeP(composite, suite.vs, func(v types.Value, r *types.Ref) bool {
		suite.False(v.Equals(suite.deadValue), "Should never have reached %+v", suite.deadValue)
		reached = append(reached, v)
		return v.Equals(suite.mustSkip)
	}, 1)
	return
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) SkipTestSkipListElement() {
	wholeList := types.NewList(suite.mustSkip, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeList)
	for _, v := range []types.Value{wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 6)
}

func (suite *WalkTestSuite) SkipTestSkipSetElement() {
	wholeSet := types.NewSet(suite.mustSkip, suite.shouldSee).Insert(suite.shouldSee)
	reached := suite.skipWorker(wholeSet)
	for _, v := range []types.Value{wholeSet, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 4)
}

func (suite *WalkTestSuite) SkipTestSkipMapValue() {
	shouldAlsoSeeItem := types.NewString("Also good")
	shouldAlsoSee := types.NewSet(shouldAlsoSeeItem)
	wholeMap := types.NewMap(suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []types.Value{wholeMap, suite.shouldSee, suite.shouldSeeItem, suite.mustSkip, shouldAlsoSee, shouldAlsoSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkTestSuite) SkipTestSkipMapKey() {
	wholeMap := types.NewMap(suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []types.Value{wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}
