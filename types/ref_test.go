package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
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
	r2 := s.Any()
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

func TestRefTypeRef(t *testing.T) {
	assert := assert.New(t)

	tr := MakeCompoundTypeRef(RefKind, MakePrimitiveTypeRef(ValueKind))

	l := NewList()
	r := NewRef(l.Ref())
	assert.True(r.Type().Equals(tr))

	cs := chunks.NewMemoryStore()
	m := NewMap()
	r2 := r.SetTargetValue(m, cs)
	assert.True(r2.Type().Equals(tr))

	b := Bool(true)
	r2 = r.SetTargetValue(b, cs)
	r2.t = MakeCompoundTypeRef(RefKind, b.Type())

	r3 := r2.SetTargetValue(Bool(false), cs)
	assert.True(r2.Type().Equals(r3.Type()))

	assert.Panics(func() { r2.SetTargetValue(Int16(1), cs) })
}

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l.Ref())
	assert.Len(r.Chunks(), 1)
	assert.Equal(l.Ref(), r.Chunks()[0])
}
