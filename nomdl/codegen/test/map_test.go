package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/types"
)

func TestMapDef(t *testing.T) {
	assert := assert.New(t)

	def := MapOfBoolToStringDef{true: "hi", false: "bye"}
	m := def.New()

	assert.Equal(uint64(2), m.Len())
	assert.Equal("hi", m.Get(true))
	assert.Equal("bye", m.Get(false))

	def2 := m.Def()
	assert.Equal(def, def2)

	m2 := NewMapOfBoolToString().Set(true, "hi").Set(false, "bye")
	assert.True(m.Equals(m2))
}

func TestValueMapDef(t *testing.T) {
	assert := assert.New(t)

	def := MapOfStringToValueDef{"s": types.NewString("s"), "i": types.Int32(42)}
	m := def.New()

	assert.Equal(uint64(2), m.Len())
	assert.True(types.NewString("s").Equals(m.Get("s")))
	assert.True(types.Int32(42).Equals(m.Get("i")))

	def2 := m.Def()
	assert.Equal(def, def2)

	m2 := NewMapOfStringToValue().Set("s", types.NewString("s")).Set("i", types.Int32(42))
	assert.True(m.Equals(m2))
}

func TestMapValue(t *testing.T) {
	assert := assert.New(t)

	def := MapOfBoolToStringDef{true: "hi", false: "bye"}
	m := def.New()
	val := m.NomsValue()
	m2 := MapOfBoolToStringFromVal(val)
	assert.True(m.Equals(m2))
}

func TestValueMapValue(t *testing.T) {
	assert := assert.New(t)

	def := MapOfStringToValueDef{"s": types.NewString("s"), "i": types.Int32(42)}
	m := def.New()
	val := m.NomsValue()
	m2 := MapOfStringToValueFromVal(val)
	assert.True(m.Equals(m2))
}
