package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	boolType := MakePrimitiveTypeRef(BoolKind)
	uint8Type := MakePrimitiveTypeRef(UInt8Kind)
	stringType := MakePrimitiveTypeRef(StringKind)
	mapType := MakeCompoundTypeRef("MapOfStringToUInt8", MapKind, stringType, uint8Type)
	setType := MakeCompoundTypeRef("SetOfString", SetKind, stringType)
	mahType := MakeStructTypeRef("MahStruct", []Field{
		Field{"Field1", stringType, false},
		Field{"Field2", boolType, false},
	}, nil)
	otherType := MakeStructTypeRef("MahOtherStruct", nil, []Field{
		Field{"StructField", mahType, false},
		Field{"StringField", stringType, false},
	})

	mRef := WriteValue(mapType, cs)
	setRef := WriteValue(setType, cs)
	otherRef := WriteValue(otherType, cs)
	mahRef := WriteValue(mahType, cs)

	assert.True(otherType.Equals(ReadValue(otherRef, cs)))
	assert.True(mapType.Equals(ReadValue(mRef, cs)))
	assert.True(setType.Equals(ReadValue(setRef, cs)))
	assert.True(mahType.Equals(ReadValue(mahRef, cs)))
}

func TestTypeWithPkgRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := PackageDef{
		Types: MapOfStringToTypeRefDef{"Spin": MakePrimitiveTypeRef(Float64Kind)},
	}.New()

	pkgRef := RegisterPackage(&pkg)
	unresolvedType := MakeTypeRef("Spin", Ref{R: pkgRef})
	unresolvedRef := WriteValue(unresolvedType, cs)

	assert.EqualValues(pkgRef, ReadValue(unresolvedRef, cs).Chunks()[0].Ref())
	assert.NotNil(ReadValue(pkgRef, cs))
}
