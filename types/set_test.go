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

	cs := chunks.NewTestStore()
	v := NewString("hello")
	r := WriteValue(v, cs)
	s := NewSet(NewRef(r), Int64(0xbeefcafe))

	assert.Len(s.Chunks(), 1)
	assert.EqualValues(r, s.Chunks()[0])
}

func TestSetIter(t *testing.T) {
	assert := assert.New(t)

	s := NewSet(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	acc := NewSet()
	s.Iter(func(v Value) bool {
		_, ok := v.(Int32)
		assert.True(ok)
		acc = acc.Insert(v)
		return false
	})
	assert.True(s.Equals(acc))

	acc = NewSet()
	s.Iter(func(v Value) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetIterAll(t *testing.T) {
	assert := assert.New(t)

	s := NewSet(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	acc := NewSet()
	s.IterAll(func(v Value) {
		_, ok := v.(Int32)
		assert.True(ok)
		acc = acc.Insert(v)
	})
	assert.True(s.Equals(acc))
}

func TestSetFilter(t *testing.T) {
	assert := assert.New(t)

	s := NewSet(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	s2 := s.Filter(func(v Value) bool {
		i, ok := v.(Int32)
		assert.True(ok)
		return i%2 == 0
	})

	assert.True(NewSet(Int32(0), Int32(2), Int32(4)).Equals(s2))
}

func TestSetTypeRef(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.TypeRef().Equals(MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(ValueKind))))

	tr := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(UInt64Kind))

	s = newSetFromData(setData{}, tr)
	assert.Equal(tr, s.TypeRef())

	s = s.Insert(UInt64(0), UInt64(1))
	assert.Equal(tr, s.TypeRef())

	s = s.Remove(UInt64(1))
	assert.Equal(tr, s.TypeRef())

	s = s.Union(s)
	assert.Equal(tr, s.TypeRef())

	s = s.Subtract(s)
	assert.Equal(tr, s.TypeRef())

	s = s.Filter(func(v Value) bool {
		return true
	})
	assert.Equal(tr, s.TypeRef())
}

func TestSetChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewSet(Int32(0))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewSet(NewRef(Int32(0).Ref()))
	c2 := l2.Chunks()
	assert.Len(c2, 1)
}
