package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestSetDef(t *testing.T) {
	assert := assert.New(t)

	def := SetOfBoolDef{true: true}
	s := def.New()

	assert.Equal(uint64(1), s.Len())
	assert.True(s.Has(true))
	assert.False(s.Has(false))

	def2 := s.Def()
	assert.Equal(def, def2)

	s2 := NewSetOfBool().Insert(true)
	assert.True(s.Equals(s2))
}

func TestSetValue(t *testing.T) {
	assert := assert.New(t)

	def := SetOfBoolDef{true: true}
	m := def.New()
	val := m.NomsValue()
	s2 := SetOfBoolFromVal(val)
	assert.True(m.Equals(s2))
}
