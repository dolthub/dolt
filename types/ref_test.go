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

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l.Ref())
	assert.Len(r.Chunks(), 1)
	assert.Equal(r, r.Chunks()[0])
}
