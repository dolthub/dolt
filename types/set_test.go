package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func TestSetLen(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Int32(1), NewString("hi"))
	assert.Equal(uint64(3), s1.Len())
	s2 := s1.Insert(Bool(false))
	assert.Equal(uint64(4), s2.Len())
	s3 := s2.Remove(Bool(true))
	assert.Equal(uint64(3), s3.Len())
}

func TestSetEmpty(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.Empty())
	s = s.Insert(Bool(false))
	assert.False(s.Empty())
	s = s.Insert(Int32(42))
	assert.False(s.Empty())
}

// BUG 98
func TestSetDuplicateInsert(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Int32(42), Int32(42))
	assert.Equal(uint64(2), s1.Len())
}

func TestSetHas(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Int32(1), NewString("hi"))
	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
	assert.True(s1.Has(Int32(1)))
	assert.False(s1.Has(Int32(0)))
	assert.True(s1.Has(NewString("hi")))
	assert.False(s1.Has(NewString("ho")))

	s2 := s1.Insert(Bool(false))
	assert.True(s2.Has(Bool(false)))
	assert.True(s2.Has(Bool(true)))

	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
}

func TestSetInsert(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Int32(0)

	assert.False(s.Has(v1))
	s = s.Insert(v1)
	assert.True(s.Has(v1))
	s = s.Insert(v2)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	s2 := s.Insert(v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.False(s.Has(v3))
	assert.True(s2.Has(v1))
	assert.True(s2.Has(v2))
	assert.True(s2.Has(v3))
}

func TestSetRemove(t *testing.T) {
	assert := assert.New(t)
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Int32(0)
	s := NewSet(v1, v2, v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s = s.Remove(v1)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s2 := s.Remove(v2)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	assert.False(s2.Has(v1))
	assert.False(s2.Has(v2))
	assert.True(s2.Has(v3))

}

func TestSetUnion(t *testing.T) {
	assert := assert.New(t)
	assert.True(NewSet(Int32(1), Int32(2)).Union(
		NewSet(Int32(2), Int32(3)),
		NewSet(Int32(-1)),
		NewSet()).Equals(
		NewSet(Int32(1), Int32(2), Int32(3), Int32(-1))))
	assert.True(NewSet(Int32(1)).Union().Equals(NewSet(Int32(1))))
}

func TestSetSubtract(t *testing.T) {
	assert := assert.New(t)
	assert.True(NewSet(Int32(-1), Int32(0), Int32(1)).Subtract(
		NewSet(Int32(0), Int32(-1)),
		NewSet(Int32(1), Int32(2))).Equals(
		NewSet()))
}

func TestSetAny(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.Nil(s.Any())
	s = s.Insert(Int32(1))
	assert.NotNil(s.Any())
	s = s.Insert(Int32(2))
	assert.NotNil(s.Any())
}

func TestSetFutures(t *testing.T) {
	assert := assert.New(t)

	cs := &chunks.TestStore{}
	v := NewString("hello")
	r := WriteValue(v, cs)
	f := futureFromRef(r)

	s := listFromFutures([]Future{f, futureFromValue(Int64(0xbeefcafe))}, cs)

	assert.Len(s.Chunks(), 1)
	assert.EqualValues(r, s.Chunks()[0].Ref())
}
