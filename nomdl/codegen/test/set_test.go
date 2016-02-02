package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestSetDef(t *testing.T) {
	assert := assert.New(t)

	def := gen.SetOfBoolDef{true: true}
	s := def.New()

	assert.Equal(uint64(1), s.Len())
	assert.True(s.Has(true))
	assert.False(s.Has(false))

	def2 := s.Def()
	assert.Equal(def, def2)

	s2 := gen.NewSetOfBool().Insert(true)
	assert.True(s.Equals(s2))
}

func TestSetOfBoolIter(t *testing.T) {
	assert := assert.New(t)

	s := gen.NewSetOfBool().Insert(true, false)
	acc := gen.NewSetOfBool()
	s.Iter(func(v bool) bool {
		acc = acc.Insert(v)
		return false
	})
	assert.True(s.Equals(acc))

	acc = gen.NewSetOfBool()
	s.Iter(func(v bool) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetOfBoolIterAll(t *testing.T) {
	assert := assert.New(t)

	s := gen.NewSetOfBool().Insert(true, false)
	acc := gen.NewSetOfBool()
	s.IterAll(func(v bool) {
		acc = acc.Insert(v)
	})
	assert.True(s.Equals(acc))
}

func TestSetOfBoolFilter(t *testing.T) {
	assert := assert.New(t)

	s := gen.NewSetOfBool().Insert(true, false)
	s2 := s.Filter(func(v bool) bool {
		return v
	})
	assert.True(gen.NewSetOfBool().Insert(true).Equals(s2))
}
