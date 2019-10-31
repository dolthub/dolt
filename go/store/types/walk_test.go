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

package types

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/dolt/go/store/chunks"
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
	err := WalkValues(context.Background(), Format_7_18, v, suite.vs, func(c Value) (stop bool) {
		actual++
		return
	})
	suite.NoError(err)
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) assertVisitedOnce(root, v Value) {
	actual := 0
	err := WalkValues(context.Background(), Format_7_18, v, suite.vs, func(c Value) bool {
		if c == v {
			actual++
		}
		return false
	})
	suite.NoError(err)
	suite.Equal(1, actual)
}

func (suite *WalkAllTestSuite) TestWalkValuesDuplicates() {
	dup := suite.NewList(Float(9), Float(10), Float(11), Float(12), Float(13))
	l := suite.NewList(Float(8), dup, dup)

	suite.assertCallbackCount(l, 11)
}

func (suite *WalkAllTestSuite) TestWalkAvoidBlobChunks() {
	buff := randomBuff(16)
	blob, err := NewBlob(context.Background(), suite.vs, bytes.NewReader(buff))
	suite.NoError(err)
	r, err := suite.vs.WriteValue(context.Background(), blob)
	suite.NoError(err)
	suite.True(r.Height() > 1)
	val, err := suite.vs.ReadValue(context.Background(), r.TargetHash())
	outBlob := val.(Blob)
	suite.NoError(err)
	suite.Equal(suite.ts.Reads(), 0)
	suite.assertCallbackCount(outBlob, 1)
	suite.Equal(suite.ts.Reads(), 0)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.assertCallbackCount(mustRef(suite.vs.WriteValue(context.Background(), Float(0.0))), 2)
	suite.assertCallbackCount(mustRef(suite.vs.WriteValue(context.Background(), String("hello"))), 2)
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
	l, err := NewList(context.Background(), suite.vs, nums...)
	suite.NoError(err)
	suite.True(mustRef(NewRef(l, Format_7_18)).Height() > 1)
	suite.assertCallbackCount(l, count+1)

	r, err := suite.vs.WriteValue(context.Background(), l)
	suite.NoError(err)
	outList, err := suite.vs.ReadValue(context.Background(), r.TargetHash())
	suite.NoError(err)
	suite.assertCallbackCount(outList, count+1)
}

func (suite *WalkAllTestSuite) TestWalkType() {
	t, err := MakeStructTypeFromFields("TestStruct", FieldMap{
		"s":  PrimitiveTypeMap[StringKind],
		"b":  PrimitiveTypeMap[BoolKind],
		"n":  PrimitiveTypeMap[FloatKind],
		"id": PrimitiveTypeMap[UUIDKind],
		"bl": PrimitiveTypeMap[BlobKind],
		"t":  PrimitiveTypeMap[TypeKind],
		"v":  PrimitiveTypeMap[ValueKind],
		"i":  PrimitiveTypeMap[IntKind],
		"u":  PrimitiveTypeMap[UintKind],
		"ib": PrimitiveTypeMap[InlineBlobKind],
	})
	suite.NoError(err)
	suite.assertVisitedOnce(t, t)
	suite.assertVisitedOnce(t, PrimitiveTypeMap[BoolKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[FloatKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[UUIDKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[IntKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[UintKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[StringKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[BlobKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[TypeKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[ValueKind])
	suite.assertVisitedOnce(t, PrimitiveTypeMap[InlineBlobKind])

	{
		t2 := mustType(MakeListType(PrimitiveTypeMap[BoolKind]))
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, PrimitiveTypeMap[BoolKind])
	}

	{
		t2 := mustType(MakeSetType(PrimitiveTypeMap[BoolKind]))
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, PrimitiveTypeMap[BoolKind])
	}

	{
		t2 := mustType(MakeRefType(PrimitiveTypeMap[BoolKind]))
		suite.assertVisitedOnce(t2, t2)
		suite.assertVisitedOnce(t2, PrimitiveTypeMap[BoolKind])
	}

	t2 := mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind]))
	suite.assertVisitedOnce(t2, t2)
	suite.assertVisitedOnce(t2, PrimitiveTypeMap[FloatKind])
	suite.assertVisitedOnce(t2, PrimitiveTypeMap[StringKind])

	t3 := mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[UUIDKind]))
	suite.assertVisitedOnce(t3, t3)
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[BoolKind])
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[FloatKind])
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[StringKind])
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[UUIDKind])
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[IntKind])
	suite.assertVisitedOnce(t3, PrimitiveTypeMap[UintKind])

	t4 := MakeCycleType("ABC")
	suite.assertVisitedOnce(t4, t4)
}

func (suite *WalkTestSuite) skipWorker(composite Value) (reached ValueSlice) {
	err := WalkValues(context.Background(), Format_7_18, composite, suite.vs, func(v Value) bool {
		suite.False(v.Equals(suite.deadValue), "Should never have reached %+v", suite.deadValue)
		reached = append(reached, v)
		return v.Equals(suite.mustSkip)
	})
	suite.NoError(err)
	return
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) TestSkipListElement() {
	wholeList, err := NewList(context.Background(), suite.vs, suite.mustSkip, suite.shouldSee, suite.shouldSee)
	suite.NoError(err)
	reached := suite.skipWorker(wholeList)
	for _, v := range []Value{wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 6)
}

func (suite *WalkTestSuite) TestSkipSetElement() {
	s, err := NewSet(context.Background(), suite.vs, suite.mustSkip, suite.shouldSee)
	suite.NoError(err)
	se, err := s.Edit().Insert(suite.shouldSee)
	suite.NoError(err)
	wholeSet, err := se.Set(context.Background())
	suite.NoError(err)
	reached := suite.skipWorker(wholeSet)
	for _, v := range []Value{wholeSet, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 4)
}

func (suite *WalkTestSuite) TestSkipMapValue() {
	shouldAlsoSeeItem := String("Also good")
	shouldAlsoSee, err := NewSet(context.Background(), suite.vs, shouldAlsoSeeItem)
	suite.NoError(err)
	wholeMap, err := NewMap(context.Background(), suite.vs, suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.shouldSee, suite.shouldSeeItem, suite.mustSkip, shouldAlsoSee, shouldAlsoSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkTestSuite) TestSkipMapKey() {
	wholeMap, err := NewMap(context.Background(), suite.vs, suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	suite.NoError(err)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.True(reached.Contains(Format_7_18, v), "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkAllTestSuite) NewList(vs ...Value) Ref {
	v, err := NewList(context.Background(), suite.vs, vs...)
	suite.NoError(err)
	ref, err := suite.vs.WriteValue(context.Background(), v)
	suite.NoError(err)
	return ref
}

func (suite *WalkAllTestSuite) NewMap(vs ...Value) Ref {
	v, err := NewMap(context.Background(), suite.vs, vs...)
	suite.NoError(err)
	ref, err := suite.vs.WriteValue(context.Background(), v)
	suite.NoError(err)
	return ref
}

func (suite *WalkAllTestSuite) NewSet(vs ...Value) Ref {
	v, err := NewSet(context.Background(), suite.vs, vs...)
	suite.NoError(err)
	ref, err := suite.vs.WriteValue(context.Background(), v)
	suite.NoError(err)
	return ref
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
	var err error
	storage := &chunks.TestStorage{}
	suite.ts = storage.NewView()
	suite.vs = NewValueStore(suite.ts)
	suite.shouldSeeItem = String("zzz")
	suite.shouldSee, err = NewList(context.Background(), suite.vs, suite.shouldSeeItem)
	suite.NoError(err)
	suite.deadValue = Float(0xDEADBEEF)
	suite.mustSkip, err = NewList(context.Background(), suite.vs, suite.deadValue)
	suite.NoError(err)
}
