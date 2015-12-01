package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestOptionalStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	str := gen.NewOptionalStruct(cs)

	_, ok := str.S()
	assert.False(ok)

	_, ok = str.B()
	assert.False(ok)

	str = str.SetS("hi")
	s, ok := str.S()
	assert.True(ok)
	assert.Equal("hi", s)

	str = str.SetB(false)
	b, ok := str.B()
	assert.True(ok)
	assert.False(b)
}

func TestOptionalStructDef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.OptionalStructDef{}
	str := def.New(cs)
	s, ok := str.S()
	assert.True(ok)
	assert.Equal("", s)

	b, ok := str.B()
	assert.True(ok)
	assert.False(b)

	def2 := str.Def()
	assert.Equal(def, def2)
}

func TestOptionalStructDefFromNew(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	str := gen.NewOptionalStruct(cs).SetB(true)
	def := str.Def()
	def2 := gen.OptionalStructDef{B: true}
	assert.Equal(def, def2)
}
