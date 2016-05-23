package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	mapType := MakeMapType(StringType, NumberType)
	setType := MakeSetType(StringType)
	mahType := MakeStructType("MahStruct", TypeMap{
		"Field1": StringType,
		"Field2": BoolType,
	})
	recType := MakeStructType("RecursiveStruct", TypeMap{
		"self": nil,
	})
	recType.Desc.(StructDesc).Fields["self"] = recType

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

	mahType := MakeStructType("MahStruct", TypeMap{
		"Field1": StringType,
		"Field2": BoolType,
	})
	assert.Equal("struct MahStruct {\n  Field1: String\n  Field2: Bool\n}", mahType.Describe())
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

func TestVerifyFieldName(t *testing.T) {
	assert := assert.New(t)

	assertInvalid := func(n string) {
		assert.Panics(func() {
			MakeStructType("S", TypeMap{n: StringType})
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

	assertValid := func(n string) {
		MakeStructType("S", TypeMap{n: StringType})
	}
	assertValid("a")
	assertValid("A")
	assertValid("a0")
	assertValid("a_")
	assertValid("a0_")
}
