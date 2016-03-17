package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefInList(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l.Ref())
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))
}

func TestRefInSet(t *testing.T) {
	assert := assert.New(t)

	s := NewSet()
	r := NewRef(s.Ref())
	s = s.Insert(r)
	r2 := s.First()
	assert.True(r.Equals(r2))
}

func TestRefInMap(t *testing.T) {
	assert := assert.New(t)

	m := NewMap()
	r := NewRef(m.Ref())
	m = m.Set(Int32(0), r).Set(r, Int32(1))
	r2 := m.Get(Int32(0))
	assert.True(r.Equals(r2))

	i := m.Get(r)
	assert.Equal(int32(1), int32(i.(Int32)))
}

func TestRefType(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	tr := MakeCompoundType(RefKind, MakePrimitiveType(ValueKind))

	l := NewList()
	r := NewRef(l.Ref())
	assert.True(r.Type().Equals(tr))

	m := NewMap()
	r2 := r.SetTargetValue(m, vs)
	assert.True(r2.Type().Equals(tr))

	b := Bool(true)
	r2 = r.SetTargetValue(b, vs)
	r2.t = MakeCompoundType(RefKind, b.Type())

	r3 := r2.SetTargetValue(Bool(false), vs)
	assert.True(r2.Type().Equals(r3.Type()))

	assert.Panics(func() { r2.SetTargetValue(Int16(1), vs) })
}

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l.Ref())
	assert.Len(r.Chunks(), 1)
	assert.Equal(l.Ref(), r.Chunks()[0])
}
