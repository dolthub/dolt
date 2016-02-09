package test

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestStructWithList(t *testing.T) {
	assert := assert.New(t)

	def := gen.StructWithListDef{
		L: gen.ListOfUint8Def{0, 1, 2},
		B: true,
		S: "world",
		I: 42,
	}

	st := def.New()
	l := st.L()
	assert.Equal(uint64(3), l.Len())

	def2 := st.Def()
	assert.Equal(def, def2)

	def2.L[2] = 22
	st2 := def2.New()
	assert.Equal(uint8(22), st2.L().Get(2))
}

func TestStructIsValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	var v types.Value = gen.StructWithListDef{
		L: gen.ListOfUint8Def{0, 1, 2},
		B: true,
		S: "world",
		I: 42,
	}.New()

	ref := types.WriteValue(v, cs)
	v2 := types.ReadValue(ref, cs)
	assert.True(v.Equals(v2))

	s2 := v2.(gen.StructWithList)
	assert.True(s2.L().Equals(gen.NewListOfUint8().Append(0, 1, 2)))
	assert.True(s2.B())
	assert.Equal("world", s2.S())
	assert.Equal(int64(42), s2.I())
}
