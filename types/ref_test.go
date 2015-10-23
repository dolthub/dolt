package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestIntentionalFail(t *testing.T) {
	assert.Fail(t, "Intentional fail to test Travis CI")
}

func TestRefInList(t *testing.T) {
	assert := assert.New(t)
	l := NewList()
	r := Ref{R: l.Ref()}
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))
}

func TestRefInSet(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	r := Ref{R: s.Ref()}
	s = s.Insert(r)
	r2 := s.Any()
	assert.True(r.Equals(r2))
}

func TestRefInMap(t *testing.T) {
	assert := assert.New(t)

	m := NewMap()
	r := Ref{R: m.Ref()}
	m = m.Set(Int32(0), r).Set(r, Int32(1))
	r2 := m.Get(Int32(0))
	assert.True(r.Equals(r2))

	i := m.Get(r)
	assert.Equal(int32(1), int32(i.(Int32)))
}

func TestRefTypeRef(t *testing.T) {
	assert := assert.New(t)
	l := NewList()
	r := Ref{R: l.Ref()}
	assert.True(r.TypeRef().Equals(MakeCompoundTypeRef("", RefKind, MakePrimitiveTypeRef(ValueKind))))
}
