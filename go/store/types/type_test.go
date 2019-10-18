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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	mapType, err := MakeMapType(StringType, FloaTType)
	assert.NoError(err)
	setType, err := MakeSetType(StringType)
	assert.NoError(err)
	mahType, err := MakeStructType("MahStruct",
		StructField{"Field1", StringType, false},
		StructField{"Field2", BoolType, false},
	)
	assert.NoError(err)
	recType, err := MakeStructType("RecursiveStruct", StructField{"self", MakeCycleType("RecursiveStruct"), false})
	assert.NoError(err)

	mRef := mustRef(vs.WriteValue(context.Background(), mapType)).TargetHash()
	setRef := mustRef(vs.WriteValue(context.Background(), setType)).TargetHash()
	mahRef := mustRef(vs.WriteValue(context.Background(), mahType)).TargetHash()
	recRef := mustRef(vs.WriteValue(context.Background(), recType)).TargetHash()

	assert.True(mapType.Equals(mustValue(vs.ReadValue(context.Background(), mRef))))
	assert.True(setType.Equals(mustValue(vs.ReadValue(context.Background(), setRef))))
	assert.True(mahType.Equals(mustValue(vs.ReadValue(context.Background(), mahRef))))
	assert.True(recType.Equals(mustValue(vs.ReadValue(context.Background(), recRef))))
}

func TestTypeType(t *testing.T) {
	assert.True(t, mustType(TypeOf(BoolType)).Equals(TypeType))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	mapType, err := MakeMapType(StringType, FloaTType)
	assert.NoError(err)
	setType, err := MakeSetType(StringType)
	assert.NoError(err)

	assert.Equal("Bool", mustString(BoolType.Describe(context.Background())))
	assert.Equal("Float", mustString(FloaTType.Describe(context.Background())))
	assert.Equal("String", mustString(StringType.Describe(context.Background())))
	assert.Equal("UUID", mustString(UUIDType.Describe(context.Background())))
	assert.Equal("Int", mustString(IntType.Describe(context.Background())))
	assert.Equal("Uint", mustString(UintType.Describe(context.Background())))
	assert.Equal("InlineBlob", mustString(InlineBlobType.Describe(context.Background())))
	assert.Equal("Map<String, Float>", mustString(mapType.Describe(context.Background())))
	assert.Equal("Set<String>", mustString(setType.Describe(context.Background())))

	mahType, err := MakeStructType("MahStruct",
		StructField{"Field1", StringType, false},
		StructField{"Field2", BoolType, false},
	)
	assert.NoError(err)
	assert.Equal("Struct MahStruct {\n  Field1: String,\n  Field2: Bool,\n}", mustString(mahType.Describe(context.Background())))
}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.True(isKindOrderedByValue(BoolType.TargetKind()))
	assert.True(isKindOrderedByValue(FloaTType.TargetKind()))
	assert.True(isKindOrderedByValue(UUIDType.TargetKind()))
	assert.True(isKindOrderedByValue(StringType.TargetKind()))
	assert.True(isKindOrderedByValue(IntType.TargetKind()))
	assert.True(isKindOrderedByValue(UintType.TargetKind()))
	assert.True(isKindOrderedByValue(InlineBlobType.TargetKind()))
	assert.True(isKindOrderedByValue(TupleKind))

	assert.False(isKindOrderedByValue(BlobType.TargetKind()))
	assert.False(isKindOrderedByValue(ValueType.TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeListType(StringType)).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeSetType(StringType)).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeMapType(StringType, ValueType)).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeRefType(StringType)).TargetKind()))
}

func TestFlattenUnionTypes(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(BoolType, mustType(MakeUnionType(BoolType)))
	assert.Equal(mustType(MakeUnionType()), mustType(MakeUnionType()))
	assert.Equal(mustType(MakeUnionType(BoolType, StringType)), mustType(MakeUnionType(BoolType, mustType(MakeUnionType(StringType)))))
	assert.Equal(mustType(MakeUnionType(BoolType, StringType, FloaTType)), mustType(MakeUnionType(BoolType, mustType(MakeUnionType(StringType, FloaTType)))))
	assert.Equal(BoolType, mustType(MakeUnionType(BoolType, BoolType)))
	assert.Equal(BoolType, mustType(MakeUnionType(BoolType, mustType(MakeUnionType()))))
	assert.Equal(BoolType, mustType(MakeUnionType(mustType(MakeUnionType()), BoolType)))
	assert.True(mustType(MakeUnionType(mustType(MakeUnionType()), mustType(MakeUnionType()))).Equals(mustType(MakeUnionType())))
	assert.Equal(mustType(MakeUnionType(BoolType, FloaTType)), mustType(MakeUnionType(BoolType, FloaTType)))
	assert.Equal(mustType(MakeUnionType(BoolType, FloaTType)), mustType(MakeUnionType(FloaTType, BoolType)))
	assert.Equal(mustType(MakeUnionType(BoolType, FloaTType)), mustType(MakeUnionType(BoolType, FloaTType, BoolType)))
	assert.Equal(mustType(MakeUnionType(BoolType, FloaTType)), mustType(MakeUnionType(mustType(MakeUnionType(BoolType, FloaTType)), FloaTType, BoolType)))
}

func TestVerifyStructFieldName(t *testing.T) {
	assert := assert.New(t)

	assertInvalid := func(n string) {
		assert.Panics(func() {
			MakeStructType("S", StructField{n, StringType, false})
		})
	}
	assertInvalid("")
	assertInvalid(" ")
	assertInvalid(" a")
	assertInvalid("a ")
	assertInvalid("0")
	assertInvalid("_")
	assertInvalid("0a")
	assertInvalid("_a")
	assertInvalid("💩")

	assertValid := func(n string) {
		MakeStructType("S", StructField{n, StringType, false})
	}
	assertValid("a")
	assertValid("A")
	assertValid("a0")
	assertValid("a_")
	assertValid("a0_")
}

func TestVerifyStructName(t *testing.T) {
	assert := assert.New(t)

	assertInvalid := func(n string) {
		assert.Panics(func() {
			MakeStructType(n)
		})
	}

	assertInvalid(" ")
	assertInvalid(" a")
	assertInvalid("a ")
	assertInvalid("0")
	assertInvalid("_")
	assertInvalid("0a")
	assertInvalid("_a")
	assertInvalid("💩")

	assertValid := func(n string) {
		MakeStructType(n)
	}
	assertValid("")
	assertValid("a")
	assertValid("A")
	assertValid("a0")
	assertValid("a_")
	assertValid("a0_")
}

func TestStructUnionWithCycles(tt *testing.T) {
	inodeType := mustType(MakeStructTypeFromFields("Inode", FieldMap{
		"attr": mustType(MakeStructTypeFromFields("Attr", FieldMap{
			"ctime": FloaTType,
			"mode":  FloaTType,
			"mtime": FloaTType,
		})),
		"contents": mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("Directory", FieldMap{
				"entries": mustType(MakeMapType(StringType, MakeCycleType("Inode"))),
			})),
			mustType(MakeStructTypeFromFields("File", FieldMap{
				"data": BlobType,
			})),
			mustType(MakeStructTypeFromFields("Symlink", FieldMap{
				"targetPath": StringType,
			})),
		)),
	}))

	vs := newTestValueStore()
	t1, _ := inodeType.Desc.(StructDesc).Field("contents")
	enc, err := EncodeValue(t1, Format_7_18)
	assert.NoError(tt, err)
	t2, err := DecodeValue(enc, vs)
	assert.NoError(tt, err)

	assert.True(tt, t1.Equals(t2))
	// Note that we cannot ensure pointer equality between t1 and t2 because the
	// types used to the construct the Unions, while eventually equivalent, are
	// not identical due to the potentially differing placement of the Cycle type.
	// We do not remake Union types after putting their component types into
	// their canonical ordering.
}

func TestHasStructCycles(tt *testing.T) {
	assert := assert.New(tt)

	assert.False(HasStructCycles(BoolType))
	assert.False(HasStructCycles(BlobType))
	assert.False(HasStructCycles(FloaTType))
	assert.False(HasStructCycles(StringType))
	assert.False(HasStructCycles(TypeType))
	assert.False(HasStructCycles(ValueType))
	assert.Panics(func() {
		HasStructCycles(MakeCycleType("Abc"))
	})

	assert.False(HasStructCycles(mustType(MakeStructType(""))))
	assert.False(HasStructCycles(mustType(MakeStructType("A"))))

	assert.True(HasStructCycles(
		mustType(MakeStructType("A", StructField{"a", mustType(MakeStructType("A")), false}))))
	assert.True(HasStructCycles(
		mustType(MakeStructType("A", StructField{"a", MakeCycleType("A"), false}))))
	assert.True(HasStructCycles(
		mustType(MakeSetType(mustType(MakeStructType("A", StructField{"a", MakeCycleType("A"), false}))))))
	assert.True(HasStructCycles(
		mustType(MakeStructType("A", StructField{"a", mustType(MakeSetType(MakeCycleType("A"))), false}))))

	assert.False(HasStructCycles(
		mustType(MakeMapType(
			mustType(MakeStructType("A")),
			mustType(MakeStructType("A")),
		)),
	))
	assert.False(HasStructCycles(
		mustType(MakeMapType(
			mustType(MakeStructType("A")),
			MakeCycleType("A"),
		)),
	))

	assert.False(HasStructCycles(
		mustType(MakeStructType("",
			StructField{
				"a",
				mustType(MakeStructType("", StructField{"b", BoolType, false})),
				false,
			},
			StructField{
				"b",
				mustType(MakeStructType("", StructField{"b", BoolType, false})),
				false},
		))),
	)
}
