package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func TestListLeafIterator(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := NewList(cs, Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	l2 := NewList(cs)
	it := newListIterator(l)
	i := 0
	for v, done := it.next(); !done; v, done = it.next() {
		l2 = l2.Append(v)
		assert.True(Int32(i).Equals(v))
		i++
	}
	assert.Equal(5, i)
	assert.True(l.Equals(l2))
}

func TestListLeafIteratorAt(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := NewList(cs, Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	l2 := NewList(cs)
	it := newListIteratorAt(l, 2)
	i := 2
	for v, done := it.next(); !done; v, done = it.next() {
		l2 = l2.Append(v)
		assert.True(Int32(i).Equals(v))
		i++
	}
	assert.Equal(5, i)
	assert.True(l.Slice(2, l.Len()).Equals(l2))
}

func TestCompoundListIterator(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	uint8List := make([]Value, 256)
	for i, _ := range uint8List {
		uint8List[i] = Uint8(i)
	}

	l := NewList(cs, uint8List...)
	l2 := NewList(cs)
	it := newListIterator(l)
	i := 0
	for v, done := it.next(); !done; v, done = it.next() {
		l2 = l2.Append(v)
		assert.True(Uint8(i).Equals(v))
		i++
	}
	assert.Equal(256, i)
	assert.True(l.Equals(l2))
}

func TestCompoundListIteratorAt(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	uint8List := make([]Value, 256)
	for i, _ := range uint8List {
		uint8List[i] = Uint8(i)
	}

	l := NewList(cs, uint8List...)
	l2 := NewList(cs)
	it := newListIteratorAt(l, 100)
	i := 100
	for v, done := it.next(); !done; v, done = it.next() {
		l2 = l2.Append(v)
		assert.True(Uint8(i).Equals(v))
		i++
	}
	assert.Equal(256, i)
	assert.True(l.Slice(100, l.Len()).Equals(l2))
}
