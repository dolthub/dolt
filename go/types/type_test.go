// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	mapType := MakeMapType(StringType, FloaTType)
	setType := MakeSetType(StringType)
	mahType := MakeStructType("MahStruct",
		StructField{"Field1", StringType, false},
		StructField{"Field2", BoolType, false},
	)
	recType := MakeStructType("RecursiveStruct", StructField{"self", MakeCycleType("RecursiveStruct"), false})

	mRef := vs.WriteValue(mapType).TargetHash()
	setRef := vs.WriteValue(setType).TargetHash()
	mahRef := vs.WriteValue(mahType).TargetHash()
	recRef := vs.WriteValue(recType).TargetHash()

	assert.True(mapType.Equals(vs.ReadValue(mRef)))
	assert.True(setType.Equals(vs.ReadValue(setRef)))
	assert.True(mahType.Equals(vs.ReadValue(mahRef)))
	assert.True(recType.Equals(vs.ReadValue(recRef)))
}

func TestTypeType(t *testing.T) {
	assert.True(t, TypeOf(BoolType).Equals(TypeType))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	mapType := MakeMapType(StringType, FloaTType)
	setType := MakeSetType(StringType)

	assert.Equal("Bool", BoolType.Describe())
	assert.Equal("Float", FloaTType.Describe())
	assert.Equal("String", StringType.Describe())
	assert.Equal("UUID", UUIDType.Describe())
	assert.Equal("Int", IntType.Describe())
	assert.Equal("Uint", UintType.Describe())
	assert.Equal("Map<String, Float>", mapType.Describe())
	assert.Equal("Set<String>", setType.Describe())

	mahType := MakeStructType("MahStruct",
		StructField{"Field1", StringType, false},
		StructField{"Field2", BoolType, false},
	)
	assert.Equal("Struct MahStruct {\n  Field1: String,\n  Field2: Bool,\n}", mahType.Describe())
}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.True(isKindOrderedByValue(BoolType.TargetKind()))
	assert.True(isKindOrderedByValue(FloaTType.TargetKind()))
	assert.True(isKindOrderedByValue(UUIDType.TargetKind()))
	assert.True(isKindOrderedByValue(StringType.TargetKind()))
	assert.True(isKindOrderedByValue(IntType.TargetKind()))
	assert.True(isKindOrderedByValue(UintType.TargetKind()))
	assert.True(isKindOrderedByValue(TupleKind))

	assert.False(isKindOrderedByValue(BlobType.TargetKind()))
	assert.False(isKindOrderedByValue(ValueType.TargetKind()))
	assert.False(isKindOrderedByValue(MakeListType(StringType).TargetKind()))
	assert.False(isKindOrderedByValue(MakeSetType(StringType).TargetKind()))
	assert.False(isKindOrderedByValue(MakeMapType(StringType, ValueType).TargetKind()))
	assert.False(isKindOrderedByValue(MakeRefType(StringType).TargetKind()))
}

func TestFlattenUnionTypes(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(BoolType, MakeUnionType(BoolType))
	assert.Equal(MakeUnionType(), MakeUnionType())
	assert.Equal(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, MakeUnionType(StringType)))
	assert.Equal(MakeUnionType(BoolType, StringType, FloaTType), MakeUnionType(BoolType, MakeUnionType(StringType, FloaTType)))
	assert.Equal(BoolType, MakeUnionType(BoolType, BoolType))
	assert.Equal(BoolType, MakeUnionType(BoolType, MakeUnionType()))
	assert.Equal(BoolType, MakeUnionType(MakeUnionType(), BoolType))
	assert.True(MakeUnionType(MakeUnionType(), MakeUnionType()).Equals(MakeUnionType()))
	assert.Equal(MakeUnionType(BoolType, FloaTType), MakeUnionType(BoolType, FloaTType))
	assert.Equal(MakeUnionType(BoolType, FloaTType), MakeUnionType(FloaTType, BoolType))
	assert.Equal(MakeUnionType(BoolType, FloaTType), MakeUnionType(BoolType, FloaTType, BoolType))
	assert.Equal(MakeUnionType(BoolType, FloaTType), MakeUnionType(MakeUnionType(BoolType, FloaTType), FloaTType, BoolType))
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
	inodeType := MakeStructTypeFromFields("Inode", FieldMap{
		"attr": MakeStructTypeFromFields("Attr", FieldMap{
			"ctime": FloaTType,
			"mode":  FloaTType,
			"mtime": FloaTType,
		}),
		"contents": MakeUnionType(
			MakeStructTypeFromFields("Directory", FieldMap{
				"entries": MakeMapType(StringType, MakeCycleType("Inode")),
			}),
			MakeStructTypeFromFields("File", FieldMap{
				"data": BlobType,
			}),
			MakeStructTypeFromFields("Symlink", FieldMap{
				"targetPath": StringType,
			}),
		),
	})

	t1, _ := inodeType.Desc.(StructDesc).Field("contents")
	t2 := DecodeValue(EncodeValue(t1), nil)

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

	assert.False(HasStructCycles(MakeStructType("")))
	assert.False(HasStructCycles(MakeStructType("A")))

	assert.True(HasStructCycles(
		MakeStructType("A", StructField{"a", MakeStructType("A"), false})))
	assert.True(HasStructCycles(
		MakeStructType("A", StructField{"a", MakeCycleType("A"), false})))
	assert.True(HasStructCycles(
		MakeSetType(MakeStructType("A", StructField{"a", MakeCycleType("A"), false}))))
	assert.True(HasStructCycles(
		MakeStructType("A", StructField{"a", MakeSetType(MakeCycleType("A")), false})))

	assert.False(HasStructCycles(
		MakeMapType(
			MakeStructType("A"),
			MakeStructType("A"),
		),
	))
	assert.False(HasStructCycles(
		MakeMapType(
			MakeStructType("A"),
			MakeCycleType("A"),
		),
	))

	assert.False(HasStructCycles(
		MakeStructType("",
			StructField{"a", MakeStructType("",
				StructField{"b", BoolType, false},
			), false},
			StructField{"b", MakeStructType("",
				StructField{"b", BoolType, false},
			), false},
		)),
	)
}
