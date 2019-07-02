// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/stretchr/testify/suite"
)

func TestWalkTestSuite(t *testing.T) {
	suite.Run(t, &WalkTestSuite{})
}

func TestWalkAllTestSuite(t *testing.T) {
	suite.Run(t, &WalkAllTestSuite{})
}

type WalkAllTestSuite struct {
	suite.Suite
	vs *ValueStore
	ts *chunks.TestStoreView
}

func (suite *WalkAllTestSuite) SetupTest() {
	storage := &chunks.TestStorage{}
	suite.ts = storage.NewView()
	suite.vs = NewValueStore(suite.ts)
}

func (suite *WalkAllTestSuite) assertCallbackCount(v Value, expected int) {
	actual := 0
	WalkValues(context.Background(), Format_7_18, v, suite.vs, func(c Value) (stop bool) {
		actual++
		return
	})
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) assertVisitedOnce(root, v Value) {
	actual := 0
	WalkValues(context.Background(), Format_7_18, v, suite.vs, func(c Value) bool {
		if c == v {
			actual++
		}
		return false
	})
	suite.Equal(1, actual)
}

func (suite *WalkAllTestSuite) TestWalkValuesDuplicates() {
	dup := suite.NewList(Float(9), Float(10), Float(11), Float(12), Float(13))
	l := suite.NewList(Float(8), dup, dup)

	suite.assertCallbackCount(l, 11)
}

func (suite *WalkAllTestSuite) TestWalkAvoidBlobChunks() {
	buff := randomBuff(16)
	// TODO(binformat)
	blob := NewBlob(context.Background(), Format_7_18, suite.vs, bytes.NewReader(buff))
	r := suite.vs.WriteValue(context.Background(), blob)
	suite.True(r.Height() > 1)
	outBlob := suite.vs.ReadValue(context.Background(), r.TargetHash()).(Blob)
	suite.Equal(suite.ts.Reads, 0)
	suite.assertCallbackCount(outBlob, 1)
	suite.Equal(suite.ts.Reads, 0)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.assertCallbackCount(suite.vs.WriteValue(context.Background(), Float(0.0)), 2)
	suite.assertCallbackCount(suite.vs.WriteValue(context.Background(), String("hello")), 2)
}

func (suite *WalkAllTestSuite) TestWalkComposites() {
	suite.assertCallbackCount(suite.NewList(), 2)
	suite.assertCallbackCount(suite.NewList(Bool(false), Float(8)), 4)
	suite.assertCallbackCount(suite.NewSet(), 2)
	suite.assertCallbackCount(suite.NewSet(Bool(false), Float(8)), 4)
	suite.assertCallbackCount(suite.NewMap(), 2)
	suite.assertCallbackCount(suite.NewMap(Float(8), Bool(true), Float(0), Bool(false)), 6)
}

func (suite *WalkAllTestSuite) TestWalkMultilevelList() {
	count := 1 << 12
	nums := make([]Value, count)
	for i := 0; i < count; i++ {
		nums[i] = Float(i)
	}
	// TODO(binformat)
	l := NewList(context.Background(), suite.vs, nums...)
	suite.True(NewRef(l, Format_7_18).Height() > 1)
	suite.assertCallbackCount(l, count+1)

	r := suite.vs.WriteValue(context.Background(), l)
	outList := suite.vs.ReadValue(context.Background(), r.TargetHash())
	suite.assertCallbackCount(outList, count+1)
}

func (suite *WalkAllTestSuite) TestWalkType() {
	t := MakeStructTypeFromFields("TestStruct", FieldMap{
		"s":  StringType,
		"b":  BoolType,
		"n":  FloaTType,
		"id": UUIDType,
		"bl": BlobType,
		"t":  TypeType,
		"v":  ValueType,
		"i":  IntType,
		"u":  UintType,
	})
	suite.assertVisitedOnce(t, t)
	suite.assertVisitedOnce(t, BoolType)
	suite.assertVisitedOnce(t, FloaTType)
	suite.assertVisitedOnce(t, UUIDType)
	suite.assertVisitedOnce(t, IntType)
	suite.assertVisitedOnce(t, UintType)
	suite.assertVisitedOnce(t, StringType)
	suite.assertVisitedOnce(t, BlobType)
	suite.assertVisitedOnce(t, TypeType)
	suite.assertVisitedOnce(t, ValueType)

	{
		t2 := MakeListType(BoolType)
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, BoolType)
	}

	{
		t2 := MakeSetType(BoolType)
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, BoolType)
	}

	{
		t2 := MakeRefType(BoolType)
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, BoolType)
	}

	t2 := MakeMapType(FloaTType, StringType)
	suite.assertVisitedOnce(t2, t2)
	suite.assertVisitedOnce(t2, FloaTType)
	suite.assertVisitedOnce(t2, StringType)

	t3 := MakeUnionType(FloaTType, StringType, BoolType, UUIDType)
	suite.assertVisitedOnce(t3, t3)
	suite.assertVisitedOnce(t3, BoolType)
	suite.assertVisitedOnce(t3, FloaTType)
	suite.assertVisitedOnce(t3, StringType)
	suite.assertVisitedOnce(t3, UUIDType)
	suite.assertVisitedOnce(t3, IntType)
	suite.assertVisitedOnce(t3, UintType)

	t4 := MakeCycleType("ABC")
	suite.assertVisitedOnce(t4, t4)
}

func (suite *WalkTestSuite) skipWorker(composite Value) (reached ValueSlice) {
	WalkValues(context.Background(), Format_7_18, composite, suite.vs, func(v Value) bool {
		suite.False(v.Equals(Format_7_18, suite.deadValue), "Should never have reached %+v", suite.deadValue)
		reached = append(reached, v)
		return v.Equals(Format_7_18, suite.mustSkip)
	})
	return
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) TestSkipListElement() {
	// TODO(binformat)
	wholeList := NewList(context.Background(), suite.vs, suite.mustSkip, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeList)
	for _, v := range []Value{wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 6)
}

func (suite *WalkTestSuite) TestSkipSetElement() {
	wholeSet := NewSet(context.Background(), Format_7_18, suite.vs, suite.mustSkip, suite.shouldSee).Edit().Insert(suite.shouldSee).Set(context.Background())
	reached := suite.skipWorker(wholeSet)
	for _, v := range []Value{wholeSet, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 4)
}

func (suite *WalkTestSuite) TestSkipMapValue() {
	shouldAlsoSeeItem := String("Also good")
	shouldAlsoSee := NewSet(context.Background(), Format_7_18, suite.vs, shouldAlsoSeeItem)
	wholeMap := NewMap(context.Background(), suite.vs, suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.shouldSee, suite.shouldSeeItem, suite.mustSkip, shouldAlsoSee, shouldAlsoSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkTestSuite) TestSkipMapKey() {
	wholeMap := NewMap(context.Background(), suite.vs, suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkAllTestSuite) NewList(vs ...Value) Ref {
	// TODO(binformat)
	v := NewList(context.Background(), suite.vs, vs...)
	return suite.vs.WriteValue(context.Background(), v)
}

func (suite *WalkAllTestSuite) NewMap(vs ...Value) Ref {
	v := NewMap(context.Background(), suite.vs, vs...)
	return suite.vs.WriteValue(context.Background(), v)
}

func (suite *WalkAllTestSuite) NewSet(vs ...Value) Ref {
	v := NewSet(context.Background(), Format_7_18, suite.vs, vs...)
	return suite.vs.WriteValue(context.Background(), v)
}

func (suite *WalkAllTestSuite) TestWalkNestedComposites() {
	suite.assertCallbackCount(suite.NewList(suite.NewSet(), Float(8)), 5)
	suite.assertCallbackCount(suite.NewSet(suite.NewList(), suite.NewSet()), 6)
	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	//  "mtlist": []
	//  "set": [5 7 8]
	//  []: "wow"
	// }
	nested := suite.NewMap(
		String("string"), String("string"),
		String("list"), suite.NewList(Bool(false), Bool(true)),
		String("map"), suite.NewMap(String("nested"), String("string")),
		String("mtlist"), suite.NewList(),
		String("set"), suite.NewSet(Float(5), Float(7), Float(8)),
		suite.NewList(), String("wow"), // note that the dupe list chunk is skipped
	)
	suite.assertCallbackCount(nested, 25)
}

type WalkTestSuite struct {
	WalkAllTestSuite
	shouldSeeItem Value
	shouldSee     Value
	mustSkip      Value
	deadValue     Value
}

func (suite *WalkTestSuite) SetupTest() {
	storage := &chunks.TestStorage{}
	suite.ts = storage.NewView()
	suite.vs = NewValueStore(suite.ts)
	suite.shouldSeeItem = String("zzz")
	// TODO(binformat)
	suite.shouldSee = NewList(context.Background(), suite.vs, suite.shouldSeeItem)
	suite.deadValue = Float(0xDEADBEEF)
	suite.mustSkip = NewList(context.Background(), suite.vs, suite.deadValue)
}
