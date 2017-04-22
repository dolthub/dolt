// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
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
	WalkValues(v, suite.vs, func(c Value) (stop bool) {
		actual++
		return
	})
	suite.Equal(expected, actual)
}

func (suite *WalkAllTestSuite) assertVisitedOnce(root, v Value) {
	actual := 0
	WalkValues(v, suite.vs, func(c Value) bool {
		if c == v {
			actual++
		}
		return false
	})
	suite.Equal(1, actual)
}

func (suite *WalkAllTestSuite) TestWalkValuesDuplicates() {
	dup := suite.NewList(Number(9), Number(10), Number(11), Number(12), Number(13))
	l := suite.NewList(Number(8), dup, dup)

	suite.assertCallbackCount(l, 11)
}

func (suite *WalkAllTestSuite) TestWalkAvoidBlobChunks() {
	buff := randomBuff(16)
	blob := NewBlob(bytes.NewReader(buff))
	r := suite.vs.WriteValue(blob)
	suite.True(r.Height() > 1)
	outBlob := suite.vs.ReadValue(r.TargetHash()).(Blob)
	suite.Equal(suite.ts.Reads, 0)
	suite.assertCallbackCount(outBlob, 1)
	suite.Equal(suite.ts.Reads, 0)
}

func (suite *WalkAllTestSuite) TestWalkPrimitives() {
	suite.assertCallbackCount(suite.vs.WriteValue(Number(0.0)), 2)
	suite.assertCallbackCount(suite.vs.WriteValue(String("hello")), 2)
}

func (suite *WalkAllTestSuite) TestWalkComposites() {
	suite.assertCallbackCount(suite.NewList(), 2)
	suite.assertCallbackCount(suite.NewList(Bool(false), Number(8)), 4)
	suite.assertCallbackCount(suite.NewSet(), 2)
	suite.assertCallbackCount(suite.NewSet(Bool(false), Number(8)), 4)
	suite.assertCallbackCount(suite.NewMap(), 2)
	suite.assertCallbackCount(suite.NewMap(Number(8), Bool(true), Number(0), Bool(false)), 6)
}

func (suite *WalkAllTestSuite) TestWalkMultilevelList() {
	count := 1 << 12
	nums := make([]Value, count)
	for i := 0; i < count; i++ {
		nums[i] = Number(i)
	}
	l := NewList(nums...)
	suite.True(NewRef(l).Height() > 1)
	suite.assertCallbackCount(l, count+1)

	r := suite.vs.WriteValue(l)
	outList := suite.vs.ReadValue(r.TargetHash())
	suite.assertCallbackCount(outList, count+1)
}

func (suite *WalkAllTestSuite) TestWalkType() {
	t := MakeStructTypeFromFields("TestStruct", FieldMap{
		"s":  StringType,
		"b":  BoolType,
		"n":  NumberType,
		"bl": BlobType,
		"t":  TypeType,
		"v":  ValueType,
	})
	suite.assertVisitedOnce(t, t)
	suite.assertVisitedOnce(t, BoolType)
	suite.assertVisitedOnce(t, NumberType)
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

	t2 := MakeMapType(NumberType, StringType)
	suite.assertVisitedOnce(t2, t2)
	suite.assertVisitedOnce(t2, NumberType)
	suite.assertVisitedOnce(t2, StringType)

	t3 := MakeUnionType(NumberType, StringType, BoolType)
	suite.assertVisitedOnce(t3, t3)
	suite.assertVisitedOnce(t3, BoolType)
	suite.assertVisitedOnce(t3, NumberType)
	suite.assertVisitedOnce(t3, StringType)

	t4 := MakeCycleType("ABC")
	suite.assertVisitedOnce(t4, t4)
}

func (suite *WalkTestSuite) skipWorker(composite Value) (reached []Value) {
	WalkValues(composite, suite.vs, func(v Value) bool {
		suite.False(v.Equals(suite.deadValue), "Should never have reached %+v", suite.deadValue)
		reached = append(reached, v)
		return v.Equals(suite.mustSkip)
	})
	return
}

// Skipping a sub-tree must allow other items in the list to be processed.
func (suite *WalkTestSuite) TestSkipListElement() {
	wholeList := NewList(suite.mustSkip, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeList)
	for _, v := range []Value{wholeList, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 6)
}

func (suite *WalkTestSuite) TestSkipSetElement() {
	wholeSet := NewSet(suite.mustSkip, suite.shouldSee).Insert(suite.shouldSee)
	reached := suite.skipWorker(wholeSet)
	for _, v := range []Value{wholeSet, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 4)
}

func (suite *WalkTestSuite) TestSkipMapValue() {
	shouldAlsoSeeItem := String("Also good")
	shouldAlsoSee := NewSet(shouldAlsoSeeItem)
	wholeMap := NewMap(suite.shouldSee, suite.mustSkip, shouldAlsoSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.shouldSee, suite.shouldSeeItem, suite.mustSkip, shouldAlsoSee, shouldAlsoSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkTestSuite) TestSkipMapKey() {
	wholeMap := NewMap(suite.mustSkip, suite.shouldSee, suite.shouldSee, suite.shouldSee)
	reached := suite.skipWorker(wholeMap)
	for _, v := range []Value{wholeMap, suite.mustSkip, suite.shouldSee, suite.shouldSeeItem} {
		suite.Contains(reached, v, "Doesn't contain %+v", v)
	}
	suite.Len(reached, 8)
}

func (suite *WalkAllTestSuite) NewList(vs ...Value) Ref {
	v := NewList(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) NewMap(vs ...Value) Ref {
	v := NewMap(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) NewSet(vs ...Value) Ref {
	v := NewSet(vs...)
	return suite.vs.WriteValue(v)
}

func (suite *WalkAllTestSuite) TestWalkNestedComposites() {
	suite.assertCallbackCount(suite.NewList(suite.NewSet(), Number(8)), 5)
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
		String("set"), suite.NewSet(Number(5), Number(7), Number(8)),
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
	suite.shouldSee = NewList(suite.shouldSeeItem)
	suite.deadValue = Number(0xDEADBEEF)
	suite.mustSkip = NewList(suite.deadValue)
}

func TestWalkDifferentStructsBasic(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	vs := NewValueStore(storage.NewView())
	assertDiffs := func(vr ValueReader, last, current Value, expectAdded, expectRemoved []Value) {
		equalIgnoreOrder := func(expect []Value, actual map[hash.Hash]Struct) {
			hashes := hash.HashSet{}
			for _, v := range expect {
				hashes.Insert(v.Hash())
			}

			assert.Equal(len(hashes), len(actual))
			for h := range actual {
				_, ok := hashes[h]
				assert.True(ok)
			}
		}

		added, removed := WalkDifferentStructs(last, current, vr)
		equalIgnoreOrder(expectAdded, added)
		equalIgnoreOrder(expectRemoved, removed)
	}

	// Nothing plus nothing is nothing
	assertDiffs(vs, nil, nil, []Value{}, []Value{})

	s1 := NewStruct("s", StructData{
		"n": Number(1),
	})

	// Top level struct.
	assertDiffs(vs, nil, s1, []Value{s1}, []Value{})

	// Same Struct
	assertDiffs(vs, s1, s1, []Value{}, []Value{})

	// Removed.
	assertDiffs(vs, s1, nil, []Value{}, []Value{s1})

	// Collections of structs
	s2 := NewStruct("s", StructData{
		"n": Number(2),
	})
	s3 := NewStruct("s", StructData{
		"n": Number(3),
	})
	s4 := NewStruct("s", StructData{
		"n": Number(4),
	})

	l1 := NewList(s2, s4)
	l2 := NewList(s1, s2, s3)

	assertDiffs(vs, l1, l2, []Value{s1, s3}, []Value{s4})
	assertDiffs(vs, l1, l1, []Value{}, []Value{})
	assertDiffs(vs, l2, l1, []Value{s4}, []Value{s1, s3})

	// Big, uncommitted collections of structs
	bl := NewList()
	i := 0
	for NewRef(bl).Height() == 1 {
		bl = bl.Append(Number(i))
		i++
	}

	l1 = bl.Append(s2, s4)
	l2 = bl.Append(s1, s2, s3)

	assertDiffs(vs, l1, l2, []Value{s1, s3}, []Value{s4})
	assertDiffs(vs, l1, l1, []Value{}, []Value{})
	assertDiffs(vs, l2, l1, []Value{s4}, []Value{s1, s3})

	// Big, committed collections of structs
	h1 := vs.WriteValue(l1).TargetHash()
	h2 := vs.WriteValue(l2).TargetHash()
	vs.persist()

	// Use a fresh value store to avoid cached chunks and ensure we're seeing the chunks persisted by the above Flush()
	ts := storage.NewView()
	nvs := NewValueStore(ts)

	l1 = nvs.ReadValue(h1).(List)
	l2 = nvs.ReadValue(h2).(List)

	ts.Reads = 0
	assertDiffs(nvs, nil, l2, []Value{s1, s2, s3}, []Value{})
	assert.Equal(1, ts.Reads) // Type information gets used to avoid loads

	ts.Reads = 0
	assertDiffs(nvs, l1, l2, []Value{s1, s3}, []Value{s4})
	assert.Equal(1, ts.Reads) // Chunk diff gets used to avoid loading common subtrees

	// Structs inside structs whose fields will be simplified away.
	s5 := NewStruct("s", StructData{
		"s1": s1,
	})
	s6 := NewStruct("s", StructData{
		"s2": s2,
	})
	s7 := NewStruct("s", StructData{
		"s3": s3,
	})

	l1 = bl.Append(s5, s6)
	l2 = bl.Append(s6, s7)

	assertDiffs(nvs, nil, l1, []Value{s5, s6, s1, s2}, []Value{})
	assertDiffs(nvs, nil, l2, []Value{s6, s7, s2, s3}, []Value{})
	assertDiffs(nvs, l1, l2, []Value{s7, s3}, []Value{s5, s1})
}
