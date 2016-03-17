package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestGenericEnumWriteRead(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDefA := MakeEnumType("EA", "aA", "bA")
	typeDefB := MakeEnumType("EB", "aB", "bB")
	pkg := NewPackage([]Type{typeDefA, typeDefB}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeA := MakeType(pkgRef, 0)
	typeB := MakeType(pkgRef, 1)

	vA := Enum{1, typeA}
	vB := Enum{1, typeB}

	assert.False(vA.Equals(vB))

	rA := WriteValue(vA, cs)
	vA2 := newValueStore(cs).ReadValue(rA)

	assert.True(vA.Equals(vA2))
	assert.True(vA2.Equals(vA))
	assert.False(vB.Equals(vA2))
	assert.False(vA2.Equals(vB))
}
