package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestSetDef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.SetOfBoolDef{true: true}
	s := def.New(cs)

	assert.Equal(uint64(1), s.Len())
	assert.True(s.Has(true))
	assert.False(s.Has(false))

	def2 := s.Def()
	assert.Equal(def, def2)

	s2 := gen.NewSetOfBool(cs).Insert(true)
	assert.True(s.Equals(s2))
}

func TestSetOfBoolIter(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	s := gen.NewSetOfBool(cs).Insert(true, false)
	acc := gen.NewSetOfBool(cs)
	s.Iter(func(v bool) bool {
		acc = acc.Insert(v)
		return false
	})
	assert.True(s.Equals(acc))

	acc = gen.NewSetOfBool(cs)
	s.Iter(func(v bool) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetOfBoolIterAll(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	s := gen.NewSetOfBool(cs).Insert(true, false)
	acc := gen.NewSetOfBool(cs)
	s.IterAll(func(v bool) {
		acc = acc.Insert(v)
	})
	assert.True(s.Equals(acc))
}

func TestSetOfBoolFilter(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	s := gen.NewSetOfBool(cs).Insert(true, false)
	s2 := s.Filter(func(v bool) bool {
		return v
	})
	assert.True(gen.NewSetOfBool(cs).Insert(true).Equals(s2))
}
