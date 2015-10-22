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
	val := m
	m2 := MapOfBoolToStringFromVal(val)
	assert.True(m.Equals(m2))
}

func TestValueMapValue(t *testing.T) {
	assert := assert.New(t)

	def := MapOfStringToValueDef{"s": types.NewString("s"), "i": types.Int32(42)}
	m := def.New()
	val := m
	m2 := MapOfStringToValueFromVal(val)
	assert.True(m.Equals(m2))
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	m := MapOfBoolToStringDef{true: "hi", false: "bye"}.New()
	acc := NewMapOfBoolToString()
	m.Iter(func(k bool, v string) bool {
		acc = acc.Set(k, v)
		return false
	})
	assert.True(m.Equals(acc))

	acc = NewMapOfBoolToString()
	m.Iter(func(k bool, v string) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestMapIterAll(t *testing.T) {
	assert := assert.New(t)
	m := MapOfBoolToStringDef{true: "hi", false: "bye"}.New()
	acc := NewMapOfBoolToString()
	m.IterAll(func(k bool, v string) {
		acc = acc.Set(k, v)
	})
	assert.True(m.Equals(acc))
}

func TestMapFilter(t *testing.T) {
	assert := assert.New(t)
	m := MapOfBoolToStringDef{true: "hi", false: "bye"}.New()
	m2 := m.Filter(func(k bool, v string) bool {
		return k
	})
	assert.True(NewMapOfBoolToString().Set(true, "hi").Equals(m2))

	m3 := m.Filter(func(k bool, v string) bool {
		return v == "bye"
	})
	assert.True(NewMapOfBoolToString().Set(false, "bye").Equals(m3))
}

func TestMapMaybeGet(t *testing.T) {
	assert := assert.New(t)
	m := NewMapOfStringToValue()
	k1 := "key1"
	k2 := "key2"
	v1 := types.NewString("SomeValue")
	m = m.Set(k1, v1)
	v, ok := m.MaybeGet(k1)
	assert.True(ok)
	assert.Equal(v1, v)
	v, ok = m.MaybeGet(k2)
	assert.False(ok)
}
