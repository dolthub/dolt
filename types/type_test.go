package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	boolType := MakePrimitiveType(BoolKind)
	uint8Type := MakePrimitiveType(Uint8Kind)
	stringType := MakePrimitiveType(StringKind)
	mapType := MakeCompoundType(MapKind, stringType, uint8Type)
	setType := MakeCompoundType(SetKind, stringType)
	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, Choices{})
	otherType := MakeStructType("MahOtherStruct", []Field{}, Choices{
		Field{"StructField", mahType, false},
		Field{"StringField", stringType, false},
	})
	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	trType := MakeType(pkgRef, 42)

	mRef := WriteValue(mapType, cs)
	setRef := WriteValue(setType, cs)
	otherRef := WriteValue(otherType, cs)
	mahRef := WriteValue(mahType, cs)
	trRef := WriteValue(trType, cs)

	assert.True(otherType.Equals(ReadValue(otherRef, cs)))
	assert.True(mapType.Equals(ReadValue(mRef, cs)))
	assert.True(setType.Equals(ReadValue(setRef, cs)))
	assert.True(mahType.Equals(ReadValue(mahRef, cs)))
	assert.True(trType.Equals(ReadValue(trRef, cs)))
}

func TestTypeWithPkgRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{MakePrimitiveType(Float64Kind)}, []ref.Ref{})

	pkgRef := RegisterPackage(&pkg)
	unresolvedType := MakeType(pkgRef, 42)
	unresolvedRef := WriteValue(unresolvedType, cs)

	v := ReadValue(unresolvedRef, cs)
	assert.EqualValues(pkgRef, v.Chunks()[0])
	assert.NotNil(ReadValue(pkgRef, cs))
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
	assert.Equal("Map(String, Uint8)", mapType.Describe())
	assert.Equal("Set(String)", setType.Describe())

	mahType := MakeStructType("MahStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, Choices{})
	assert.Equal("struct MahStruct {\n  Field1: String\n  Field2: optional Bool\n}", mahType.Describe())

	otherType := MakeStructType("MahOtherStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, true},
	}, Choices{
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
	assert.False(MakeCompoundType(RefKind, MakePrimitiveType(StringKind)).IsOrdered())
}
