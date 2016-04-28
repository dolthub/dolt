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
	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", StringType, false},
		Field{"Field2", BoolType, true},
	}, []Field{})
	otherType := MakeStructType("MahOtherStruct", []Field{}, []Field{
		Field{"StructField", mahType, false},
		Field{"StringField", StringType, false},
	})
	recType := MakeStructType("RecursiveStruct", []Field{
		Field{Name: "self", T: nil},
	}, []Field{})
	recType.Desc.(StructDesc).Fields[0].T = recType

	mRef := vs.WriteValue(mapType).TargetRef()
	setRef := vs.WriteValue(setType).TargetRef()
	otherRef := vs.WriteValue(otherType).TargetRef()
	mahRef := vs.WriteValue(mahType).TargetRef()
	recRef := vs.WriteValue(recType).TargetRef()

	assert.True(otherType.Equals(vs.ReadValue(otherRef)))
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

	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", StringType, false},
		Field{"Field2", BoolType, true},
	}, []Field{})
	assert.Equal("struct MahStruct {\n  Field1: String\n  Field2: optional Bool\n}", mahType.Describe())

	otherType := MakeStructType("MahOtherStruct", []Field{
		Field{"Field1", StringType, false},
		Field{"Field2", BoolType, true},
	}, []Field{
		Field{"NumberField", NumberType, false},
		Field{"StringField", StringType, false},
	})
	assert.Equal("struct MahOtherStruct {\n  Field1: String\n  Field2: optional Bool\n  union {\n    NumberField: Number\n    StringField: String\n  }\n}", otherType.Describe())

}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.False(BoolType.IsOrdered())
	assert.True(NumberType.IsOrdered())
	assert.True(StringType.IsOrdered())
	assert.False(BlobType.IsOrdered())
	assert.False(ValueType.IsOrdered())
	assert.False(MakeListType(StringType).IsOrdered())
	assert.False(MakeSetType(StringType).IsOrdered())
	assert.False(MakeMapType(StringType, ValueType).IsOrdered())
	assert.True(MakeRefType(StringType).IsOrdered())
}
