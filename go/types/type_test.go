// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	mapType := MakeMapType(StringType, NumberType)
	setType := MakeSetType(StringType)
	mahType := MakeStructType("MahStruct",
		[]string{"Field1", "Field2"},
		[]*Type{
			StringType,
			BoolType,
		},
	)
	recType := MakeStructType("RecursiveStruct", []string{"self"}, []*Type{MakeCycleType(0)})

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
	assert.True(t, BoolType.Type().Equals(TypeType))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	mapType := MakeMapType(StringType, NumberType)
	setType := MakeSetType(StringType)

	assert.Equal("Bool", BoolType.Describe())
	assert.Equal("Number", NumberType.Describe())
	assert.Equal("String", StringType.Describe())
	assert.Equal("Map<String, Number>", mapType.Describe())
	assert.Equal("Set<String>", setType.Describe())

	mahType := MakeStructType("MahStruct",
		[]string{"Field1", "Field2"},
		[]*Type{
			StringType,
			BoolType,
		},
	)
	assert.Equal("struct MahStruct {\n  Field1: String,\n  Field2: Bool,\n}", mahType.Describe())
}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.True(isKindOrderedByValue(BoolType.Kind()))
	assert.True(isKindOrderedByValue(NumberType.Kind()))
	assert.True(isKindOrderedByValue(StringType.Kind()))
	assert.False(isKindOrderedByValue(BlobType.Kind()))
	assert.False(isKindOrderedByValue(ValueType.Kind()))
	assert.False(isKindOrderedByValue(MakeListType(StringType).Kind()))
	assert.False(isKindOrderedByValue(MakeSetType(StringType).Kind()))
	assert.False(isKindOrderedByValue(MakeMapType(StringType, ValueType).Kind()))
	assert.False(isKindOrderedByValue(MakeRefType(StringType).Kind()))
}

func TestFlattenUnionTypes(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(BoolType, MakeUnionType(BoolType))
	assert.Equal(MakeUnionType(), MakeUnionType())
	assert.Equal(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, MakeUnionType(StringType)))
	assert.Equal(MakeUnionType(BoolType, StringType, NumberType), MakeUnionType(BoolType, MakeUnionType(StringType, NumberType)))
	assert.Equal(BoolType, MakeUnionType(BoolType, BoolType))
	assert.Equal(BoolType, MakeUnionType(BoolType, MakeUnionType()))
	assert.Equal(BoolType, MakeUnionType(MakeUnionType(), BoolType))
	assert.True(MakeUnionType(MakeUnionType(), MakeUnionType()).Equals(MakeUnionType()))
	assert.Equal(MakeUnionType(BoolType, NumberType), MakeUnionType(BoolType, NumberType))
	assert.Equal(MakeUnionType(BoolType, NumberType), MakeUnionType(NumberType, BoolType))
	assert.Equal(MakeUnionType(BoolType, NumberType), MakeUnionType(BoolType, NumberType, BoolType))
	assert.Equal(MakeUnionType(BoolType, NumberType), MakeUnionType(MakeUnionType(BoolType, NumberType), NumberType, BoolType))
}

func TestVerifyStructFieldName(t *testing.T) {
	assert := assert.New(t)

	assertInvalid := func(n string) {
		assert.Panics(func() {
			MakeStructType("S", []string{n}, []*Type{StringType})
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
		MakeStructType("S", []string{n}, []*Type{StringType})
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
			MakeStructType(n, []string{}, []*Type{})
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
		MakeStructType(n, []string{}, []*Type{})
	}
	assertValid("")
	assertValid("a")
	assertValid("A")
	assertValid("a0")
	assertValid("a_")
	assertValid("a0_")
}

func TestUnionWithCycles(tt *testing.T) {
	inodeType := MakeStructType("Inode", []string{"attr", "contents"}, []*Type{
		MakeStructType("Attr", []string{"ctime", "mode", "mtime"}, []*Type{NumberType, NumberType, NumberType}),
		MakeUnionType(MakeStructType("Directory", []string{"entries"}, []*Type{MakeMapType(StringType, MakeCycleType(1))}),
			MakeStructType("File", []string{"data"}, []*Type{BlobType}),
			MakeStructType("Symlink", []string{"targetPath"}, []*Type{StringType}),
		),
	})

	t1 := inodeType.Desc.(StructDesc).Field("contents")
	t2 := DecodeValue(EncodeValue(t1, nil), nil)

	assert.True(tt, t1.Equals(t2))
	// Note that we cannot ensure pointer equality between t1 and t2 because the types used to the construct the Unions, while eventually equivalent, are not identical due to the potentially differing placement of the Cycle type. We do not remake Union types after putting their component types into their canonical ordering.
}
