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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	mapType, err := MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	setType, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(t, err)
	mahType, err := MakeStructType("MahStruct",
		StructField{"Field1", PrimitiveTypeMap[StringKind], false},
		StructField{"Field2", PrimitiveTypeMap[BoolKind], false},
	)
	require.NoError(t, err)
	recType, err := MakeStructType("RecursiveStruct", StructField{"self", MakeCycleType("RecursiveStruct"), false})
	require.NoError(t, err)

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
	assert.True(t, mustType(TypeOf(PrimitiveTypeMap[BoolKind])).Equals(PrimitiveTypeMap[TypeKind]))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	mapType, err := MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	setType, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(t, err)

	assert.Equal("Bool", mustString(PrimitiveTypeMap[BoolKind].Describe(context.Background())))
	assert.Equal("Float", mustString(PrimitiveTypeMap[FloatKind].Describe(context.Background())))
	assert.Equal("String", mustString(PrimitiveTypeMap[StringKind].Describe(context.Background())))
	assert.Equal("UUID", mustString(PrimitiveTypeMap[UUIDKind].Describe(context.Background())))
	assert.Equal("Int", mustString(PrimitiveTypeMap[IntKind].Describe(context.Background())))
	assert.Equal("Uint", mustString(PrimitiveTypeMap[UintKind].Describe(context.Background())))
	assert.Equal("InlineBlob", mustString(PrimitiveTypeMap[InlineBlobKind].Describe(context.Background())))
	assert.Equal("Decimal", mustString(PrimitiveTypeMap[DecimalKind].Describe(context.Background())))
	assert.Equal("Map<String, Float>", mustString(mapType.Describe(context.Background())))
	assert.Equal("Set<String>", mustString(setType.Describe(context.Background())))

	mahType, err := MakeStructType("MahStruct",
		StructField{"Field1", PrimitiveTypeMap[StringKind], false},
		StructField{"Field2", PrimitiveTypeMap[BoolKind], false},
	)
	require.NoError(t, err)
	assert.Equal("Struct MahStruct {\n  Field1: String,\n  Field2: Bool,\n}", mustString(mahType.Describe(context.Background())))
}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[BoolKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[FloatKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[UUIDKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[StringKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[IntKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[UintKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[InlineBlobKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[DecimalKind].TargetKind()))
	assert.True(isKindOrderedByValue(TupleKind))

	assert.False(isKindOrderedByValue(PrimitiveTypeMap[BlobKind].TargetKind()))
	assert.False(isKindOrderedByValue(PrimitiveTypeMap[ValueKind].TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeListType(PrimitiveTypeMap[StringKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeSetType(PrimitiveTypeMap[StringKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[ValueKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeRefType(PrimitiveTypeMap[StringKind])).TargetKind()))
}

func TestFlattenUnionTypes(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind])))
	assert.Equal(mustType(MakeUnionType()), mustType(MakeUnionType()))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[StringKind])))))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])))))
	assert.Equal(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])))
	assert.Equal(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType()))))
	assert.Equal(PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(mustType(MakeUnionType()), PrimitiveTypeMap[BoolKind])))
	assert.True(mustType(MakeUnionType(mustType(MakeUnionType()), mustType(MakeUnionType()))).Equals(mustType(MakeUnionType())))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))
	assert.Equal(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), mustType(MakeUnionType(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))
}

func TestVerifyStructFieldName(t *testing.T) {
	assert := assert.New(t)

	assertInvalid := func(n string) {
		assert.Panics(func() {
			MakeStructType("S", StructField{n, PrimitiveTypeMap[StringKind], false})
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
		MakeStructType("S", StructField{n, PrimitiveTypeMap[StringKind], false})
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
			"ctime": PrimitiveTypeMap[FloatKind],
			"mode":  PrimitiveTypeMap[FloatKind],
			"mtime": PrimitiveTypeMap[FloatKind],
		})),
		"contents": mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("Directory", FieldMap{
				"entries": mustType(MakeMapType(PrimitiveTypeMap[StringKind], MakeCycleType("Inode"))),
			})),
			mustType(MakeStructTypeFromFields("File", FieldMap{
				"data": PrimitiveTypeMap[BlobKind],
			})),
			mustType(MakeStructTypeFromFields("Symlink", FieldMap{
				"targetPath": PrimitiveTypeMap[StringKind],
			})),
		)),
	}))

	vs := newTestValueStore()
	t1, _ := inodeType.Desc.(StructDesc).Field("contents")
	enc, err := EncodeValue(t1, vs.Format())
	require.NoError(tt, err)
	t2, err := DecodeValue(enc, vs)
	require.NoError(tt, err)

	assert.True(tt, t1.Equals(t2))
	// Note that we cannot ensure pointer equality between t1 and t2 because the
	// types used to the construct the Unions, while eventually equivalent, are
	// not identical due to the potentially differing placement of the Cycle type.
	// We do not remake Union types after putting their component types into
	// their canonical ordering.
}

func TestHasStructCycles(tt *testing.T) {
	assert := assert.New(tt)

	assert.False(HasStructCycles(PrimitiveTypeMap[BoolKind]))
	assert.False(HasStructCycles(PrimitiveTypeMap[BlobKind]))
	assert.False(HasStructCycles(PrimitiveTypeMap[FloatKind]))
	assert.False(HasStructCycles(PrimitiveTypeMap[StringKind]))
	assert.False(HasStructCycles(PrimitiveTypeMap[TypeKind]))
	assert.False(HasStructCycles(PrimitiveTypeMap[ValueKind]))
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
				mustType(MakeStructType("", StructField{"b", PrimitiveTypeMap[BoolKind], false})),
				false,
			},
			StructField{
				"b",
				mustType(MakeStructType("", StructField{"b", PrimitiveTypeMap[BoolKind], false})),
				false},
		))),
	)
}
