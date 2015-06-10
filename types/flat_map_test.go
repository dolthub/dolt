package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.IsType(flatMap{}, m)
	assert.Equal(uint64(0), m.Len())
	m = NewMap("foo", NewString("foo"), "bar", NewString("bar"))
	assert.Equal(uint64(2), m.Len())
	assert.True(NewString("foo").Equals(m.Get("foo")))
	assert.True(NewString("bar").Equals(m.Get("bar")))
}

func TestFlatMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	assert.False(m1.Has("foo"))
	m2 := m1.Set("foo", NewString("foo"))
	assert.False(m1.Has("foo"))
	assert.True(m2.Has("foo"))
	m3 := m1.Remove("foo")
	assert.False(m1.Has("foo"))
	assert.True(m2.Has("foo"))
	assert.False(m3.Has("foo"))
}

func TestFlatMapSetGet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	assert.Nil(m1.Get("foo"))
	m2 := m1.Set("foo", Int32(42))
	assert.Nil(m1.Get("foo"))
	assert.True(Int32(42).Equals(m2.Get("foo")))
	m3 := m2.Set("foo", Int32(43))
	assert.Nil(m1.Get("foo"))
	assert.True(Int32(42).Equals(m2.Get("foo")))
	assert.True(Int32(43).Equals(m3.Get("foo")))
	m4 := m3.Remove("foo")
	assert.Nil(m1.Get("foo"))
	assert.True(Int32(42).Equals(m2.Get("foo")))
	assert.True(Int32(43).Equals(m3.Get("foo")))
	assert.Nil(m4.Get("foo"))
}

func TestFlatMapSetM(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1.SetM()
	assert.True(m1.Equals(m2))
	m3 := m2.SetM("foo", NewString("bar"), "hot", NewString("dog"))
	assert.Equal(uint64(2), m3.Len())
	assert.True(NewString("bar").Equals(m3.Get("foo")))
	assert.True(NewString("dog").Equals(m3.Get("hot")))
	m4 := m3.SetM("mon", NewString("key"))
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
}

func TestFlatMapIter(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	got := map[string]Value{}
	stop := false
	cb := func(k string, v Value) bool {
		got[k] = v
		return stop
	}

	m.Iter(cb)
	assert.Equal(0, len(got))

	m = m.SetM("a", Int32(0), "b", Int32(1))
	m.Iter(cb)
	assert.Equal(2, len(got))
	assert.True(Int32(0).Equals(got["a"]))
	assert.True(Int32(1).Equals(got["b"]))

	got = map[string]Value{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(got))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(Int32(0).Equals(got["a"]) || Int32(1).Equals(got["b"]))
}

func TestFlatMapEquals(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1
	m3 := NewMap()

	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.True(m3.Equals(m2))
	assert.True(m2.Equals(m3))

	m1 = NewMap("foo", Float32(0.0), "bar", Float32(1.1))
	m2 = m2.SetM("foo", Float32(0.0), "bar", Float32(1.1))
	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.False(m2.Equals(m3))
	assert.False(m3.Equals(m2))
}
