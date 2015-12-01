package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/attic-labs/noms/types"
)

func TestEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.EnumStructDef{gen.Right}
	st := def.New(cs)

	def2 := st.Def()
	st2 := def.New(cs)

	assert.Equal(def, def2)
	assert.True(st.Equals(st2))

	st3 := gen.NewEnumStruct(cs)
	assert.Equal(gen.Right, st3.Hand())
	st3 = st3.SetHand(gen.Left)
	assert.Equal(gen.Left, st3.Hand())
}

func TestEnumValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.EnumStructDef{gen.Switch}
	var st types.Value
	st = def.New(cs)
	st2 := st.(gen.EnumStruct)
	assert.True(st.Equals(st2))
}

func TestEnumIsValue(t *testing.T) {
	cs := chunks.NewMemoryStore()
	var v types.Value = gen.NewEnumStruct(cs)
	ref := types.WriteValue(v, cs)
	v2 := types.ReadValue(ref, cs)
	assert.True(t, v.Equals(v2))
}
