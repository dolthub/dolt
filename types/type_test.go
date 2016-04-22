package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	boolType := MakePrimitiveType(BoolKind)
	uint8Type := MakePrimitiveType(Uint8Kind)
	stringType := MakePrimitiveType(StringKind)
	mapType := MakeCompoundType(MapKind, stringType, uint8Type)
	setType := MakeCompoundType(SetKind, stringType)
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

	pkg := NewPackage([]*Type{MakePrimitiveType(Float64Kind)}, []ref.Ref{})

	pkgRef := RegisterPackage(&pkg)
	unresolvedType := MakeType(pkgRef, 42)
	unresolvedRef := vs.WriteValue(unresolvedType).TargetRef()

	v := vs.ReadValue(unresolvedRef)
	assert.EqualValues(pkgRef, v.Chunks()[0].TargetRef())
	assert.NotNil(vs.ReadValue(pkgRef))
}

func TestTypeType(t *testing.T) {
	assert.True(t, MakePrimitiveType(BoolKind).Type().Equals(MakePrimitiveType(TypeKind)))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	boolType := MakePrimitiveType(BoolKind)
	uint8Type := MakePrimitiveType(Uint8Kind)
	stringType := MakePrimitiveType(StringKind)
	mapType := MakeCompoundType(MapKind, stringType, uint8Type)
	setType := MakeCompoundType(SetKind, stringType)

	assert.Equal("Bool", boolType.Describe())
	assert.Equal("Uint8", uint8Type.Describe())
	assert.Equal("String", stringType.Describe())
	assert.Equal("Map<String, Uint8>", mapType.Describe())
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
		Field{"Uint8Field", uint8Type, false},
		Field{"StringField", stringType, false},
	})
	assert.Equal("struct MahOtherStruct {\n  Field1: String\n  Field2: optional Bool\n  union {\n    Uint8Field: Uint8\n    StringField: String\n  }\n}", otherType.Describe())

}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.False(MakePrimitiveType(BoolKind).IsOrdered())
	assert.True(MakePrimitiveType(Uint8Kind).IsOrdered())
	assert.True(MakePrimitiveType(Uint16Kind).IsOrdered())
	assert.True(MakePrimitiveType(Uint32Kind).IsOrdered())
	assert.True(MakePrimitiveType(Uint64Kind).IsOrdered())
	assert.True(MakePrimitiveType(Int8Kind).IsOrdered())
	assert.True(MakePrimitiveType(Int16Kind).IsOrdered())
	assert.True(MakePrimitiveType(Int32Kind).IsOrdered())
	assert.True(MakePrimitiveType(Int64Kind).IsOrdered())
	assert.True(MakePrimitiveType(Float32Kind).IsOrdered())
	assert.True(MakePrimitiveType(Float64Kind).IsOrdered())
	assert.True(MakePrimitiveType(StringKind).IsOrdered())
	assert.False(MakePrimitiveType(BlobKind).IsOrdered())
	assert.False(MakePrimitiveType(ValueKind).IsOrdered())
	assert.False(MakeCompoundType(ListKind, MakePrimitiveType(StringKind)).IsOrdered())
	assert.False(MakeCompoundType(SetKind, MakePrimitiveType(StringKind)).IsOrdered())
	assert.False(MakeCompoundType(MapKind, MakePrimitiveType(StringKind), MakePrimitiveType(ValueKind)).IsOrdered())
	assert.True(MakeCompoundType(RefKind, MakePrimitiveType(StringKind)).IsOrdered())
}
