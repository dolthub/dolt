package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestListLeafIterator(t *testing.T) {
	assert := assert.New(t)

	l := NewList(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	l2 := NewList()
	it := newListIterator(l)
	i := 0
	for f, done := it.next(); !done; f, done = it.next() {
		l2 = l2.Append(f.Deref(nil))
		assert.True(Int32(i).Equals(f.Deref(nil)))
		i++
	}
	assert.Equal(5, i)
	assert.True(l.Equals(l2))
}

func TestListLeafIteratorAt(t *testing.T) {
	assert := assert.New(t)

	l := NewList(Int32(0), Int32(1), Int32(2), Int32(3), Int32(4))
	l2 := NewList()
	it := newListIteratorAt(l, 2)
	i := 2
	for f, done := it.next(); !done; f, done = it.next() {
		l2 = l2.Append(f.Deref(nil))
		assert.True(Int32(i).Equals(f.Deref(nil)))
		i++
	}
	assert.Equal(5, i)
	assert.True(l.Slice(2, l.Len()).Equals(l2))
}

func TestCompoundListIterator(t *testing.T) {
	assert := assert.New(t)

	l := getTestCompoundList(t)
	l2 := NewList()
	it := newListIterator(l)
	i := 0
	for f, done := it.next(); !done; f, done = it.next() {
		l2 = l2.Append(f.Deref(nil))
		assert.True(UInt8(i).Equals(f.Deref(nil)))
		i++
	}
	assert.Equal(255, i)
	assert.True(l.Equals(l2))
}

func TestCompoundListIteratorAt(t *testing.T) {
	assert := assert.New(t)

	l := getTestCompoundList(t)
	l2 := NewList()
	it := newListIteratorAt(l, 100)
	i := 100
	for f, done := it.next(); !done; f, done = it.next() {
		l2 = l2.Append(f.Deref(nil))
		assert.True(UInt8(i).Equals(f.Deref(nil)))
		i++
	}
	assert.Equal(255, i)
	assert.True(l.Slice(100, l.Len()).Equals(l2))
}
