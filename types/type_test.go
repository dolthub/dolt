package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	boolType := BoolType
	numberType := NumberType
	stringType := StringType
	mapType := MakeMapType(stringType, numberType)
	setType := MakeSetType(stringType)
	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, []Field{})
	otherType := MakeStructType("MahOtherStruct", []Field{}, []Field{
		Field{"StructField", mahType, false},
		Field{"StringField", stringType, false},
	})
	pkgRef := vs.WriteValue(NewPackage([]*Type{}, ref.RefSlice{})).TargetRef()
	trType := MakeType(pkgRef, 42)

	mRef := vs.WriteValue(mapType).TargetRef()
	setRef := vs.WriteValue(setType).TargetRef()
	otherRef := vs.WriteValue(otherType).TargetRef()
	mahRef := vs.WriteValue(mahType).TargetRef()
	trRef := vs.WriteValue(trType).TargetRef()

	assert.True(otherType.Equals(vs.ReadValue(otherRef)))
	assert.True(mapType.Equals(vs.ReadValue(mRef)))
	assert.True(setType.Equals(vs.ReadValue(setRef)))
	assert.True(mahType.Equals(vs.ReadValue(mahRef)))
	assert.True(trType.Equals(vs.ReadValue(trRef)))
}

func TestTypeWithPkgRef(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	pkg := NewPackage([]*Type{NumberType}, []ref.Ref{})

	pkgRef := RegisterPackage(&pkg)
	unresolvedType := MakeType(pkgRef, 42)
	unresolvedRef := vs.WriteValue(unresolvedType).TargetRef()

	v := vs.ReadValue(unresolvedRef)
	assert.EqualValues(pkgRef, v.Chunks()[0].TargetRef())
	assert.NotNil(vs.ReadValue(pkgRef))
}

func TestTypeType(t *testing.T) {
	assert.True(t, BoolType.Type().Equals(TypeType))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	boolType := BoolType
	numberType := NumberType
	stringType := StringType
	mapType := MakeMapType(stringType, numberType)
	setType := MakeSetType(stringType)

	assert.Equal("Bool", boolType.Describe())
	assert.Equal("Number", numberType.Describe())
	assert.Equal("String", stringType.Describe())
	assert.Equal("Map<String, Number>", mapType.Describe())
	assert.Equal("Set<String>", setType.Describe())

	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, []Field{})
	assert.Equal("struct MahStruct {\n  Field1: String\n  Field2: optional Bool\n}", mahType.Describe())

	otherType := MakeStructType("MahOtherStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, []Field{
		Field{"NumberField", numberType, false},
		Field{"StringField", stringType, false},
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
