package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefInList(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l)
	l = l.Append(r)
	r2 := l.Get(0)
	assert.True(r.Equals(r2))
}

func TestRefInSet(t *testing.T) {
	assert := assert.New(t)

	s := NewSet()
	r := NewRef(s)
	s = s.Insert(r)
	r2 := s.First()
	assert.True(r.Equals(r2))
}

func TestRefInMap(t *testing.T) {
	assert := assert.New(t)

	m := NewMap()
	r := NewRef(m)
	m = m.Set(Number(0), r).Set(r, Number(1))
	r2 := m.Get(Number(0))
	assert.True(r.Equals(r2))

	i := m.Get(r)
	assert.Equal(int32(1), int32(i.(Number)))
}

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	r := NewRef(l)
	assert.Len(r.Chunks(), 1)
	assert.Equal(r, r.Chunks()[0])
}
