package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestGenericEnumWriteRead(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDefA := MakeEnumTypeRef("EA", "aA", "bA")
	typeDefB := MakeEnumTypeRef("EB", "aB", "bB")
	pkg := NewPackage([]Type{typeDefA, typeDefB}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRefA := MakeTypeRef(pkgRef, 0)
	typeRefB := MakeTypeRef(pkgRef, 1)

	vA := Enum{1, typeRefA}
	vB := Enum{1, typeRefB}

	assert.False(vA.Equals(vB))

	rA := WriteValue(vA, cs)
	vA2 := ReadValue(rA, cs)

	assert.True(vA.Equals(vA2))
	assert.True(vA2.Equals(vA))
	assert.False(vB.Equals(vA2))
	assert.False(vA2.Equals(vB))
}
