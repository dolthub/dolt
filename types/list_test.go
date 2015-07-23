package types

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestListLen(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	assert.Equal(uint64(0), l.Len())
	l = l.Append(Bool(true))
	assert.Equal(uint64(1), l.Len())
	l = l.Append(Bool(false), Bool(false))
	assert.Equal(uint64(3), l.Len())
}

func TestListEmpty(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	assert.True(l.Empty())
	l = l.Append(Bool(true))
	assert.False(l.Empty())
	l = l.Append(Bool(false), Bool(false))
	assert.False(l.Empty())
}

func TestListGet(t *testing.T) {
	assert := assert.New(t)

	l := NewList()
	l = l.Append(Int32(0), Int32(1), Int32(2))
	assert.Equal(Int32(0), l.Get(0))
	assert.Equal(Int32(1), l.Get(1))
	assert.Equal(Int32(2), l.Get(2))

	assert.Panics(func() {
		l.Get(3)
	})
}

func TestListSlice(t *testing.T) {
	assert := assert.New(t)
	l1 := NewList()
	l1 = l1.Append(Int32(0), Int32(1), Int32(2), Int32(3))
	l2 := l1.Slice(1, 3)
	assert.Equal(uint64(4), l1.Len())
	assert.Equal(uint64(2), l2.Len())
	assert.Equal(Int32(1), l2.Get(0))
	assert.Equal(Int32(2), l2.Get(1))

	l3 := l1.Slice(0, 0)
	assert.Equal(uint64(0), l3.Len())
	l3 = l1.Slice(1, 1)
	assert.Equal(uint64(0), l3.Len())
	l3 = l1.Slice(1, 2)
	assert.Equal(uint64(1), l3.Len())
	assert.Equal(Int32(1), l3.Get(0))
	l3 = l1.Slice(0, l1.Len())
	assert.True(l1.Equals(l3))

	assert.Panics(func() {
		l3 = l1.Slice(0, l1.Len()+1)
	})
}

func TestListSet(t *testing.T) {
	assert := assert.New(t)
	l0 := NewList()
	l0 = l0.Append(Float32(0.0))
	l1 := l0.Set(uint64(0), Float32(1.0))
	assert.Equal(Float32(1.0), l1.Get(0))
	assert.Equal(Float32(0.0), l0.Get(0))
	assert.Panics(func() {
		l1.Set(uint64(2), Float32(2.0))
	})
}

func TestListAppend(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l1 := l0.Append(Bool(false))
	assert.Equal(uint64(0), l0.Len())
	assert.Equal(uint64(1), l1.Len())
	assert.Equal(Bool(false), l1.Get(0))

	// Append(v1, v2)
	l2 := l1.Append(Bool(true), Bool(true))
	assert.Equal(uint64(3), l2.Len())
	assert.Equal(Bool(false), l2.Get(0))
	assert.True(NewList(Bool(true), Bool(true)).Equals(l2.Slice(1, l2.Len())))
	assert.Equal(uint64(1), l1.Len())
}

func TestListInsert(t *testing.T) {
	assert := assert.New(t)

	// Insert(0, v1)
	l0 := NewList()
	l1 := l0.Insert(uint64(0), Int32(-1))
	assert.Equal(uint64(0), l0.Len())
	assert.Equal(uint64(1), l1.Len())
	assert.Equal(Int32(-1), l1.Get(0))

	// Insert(0, v1, v2)
	l2 := l1.Insert(0, Int32(-3), Int32(-2))
	assert.Equal(uint64(3), l2.Len())
	assert.Equal(Int32(-1), l2.Get(2))
	assert.True(NewList(Int32(-3), Int32(-2)).Equals(l2.Slice(0, 2)))
	assert.Equal(uint64(1), l1.Len())

	// Insert(2, v3)
	l3 := l2.Insert(2, Int32(1))
	assert.Equal(Int32(1), l3.Get(2))

	assert.Panics(func() {
		l2.Insert(5, Int32(0))
	})
}

func TestListRemove(t *testing.T) {
	assert := assert.New(t)
	l0 := NewList()
	l0 = l0.Remove(0, 0)
	assert.Equal(uint64(0), l0.Len())

	l0 = l0.Append(Bool(false), Bool(true), Bool(true), Bool(false))
	l1 := l0.Remove(1, 3)
	assert.Equal(uint64(4), l0.Len())
	assert.Equal(uint64(2), l1.Len())
	assert.True(NewList(Bool(false), Bool(false)).Equals(l1))

	l1 = l1.Remove(1, 2)
	assert.True(NewList(Bool(false)).Equals(l1))

	l1 = l1.Remove(0, 1)
	assert.True(NewList().Equals(l1))

	assert.Panics(func() {
		l1.Remove(0, 1)
	})
}

func TestListRemoveAt(t *testing.T) {
	assert := assert.New(t)
	l0 := NewList()
	l0 = l0.Append(Bool(false), Bool(true))
	l1 := l0.RemoveAt(1)
	assert.True(NewList(Bool(false)).Equals(l1))
	l1 = l1.RemoveAt(0)
	assert.True(NewList().Equals(l1))

	assert.Panics(func() {
		l1.RemoveAt(0)
	})
}

func TestListFutures(t *testing.T) {
	assert := assert.New(t)

	cs := &chunks.TestStore{}
	v := NewString("hello")
	r, _ := WriteValue(v, cs)
	f := FutureFromRef(r)

	l := listFromFutures([]Future{f, FutureFromValue(Int64(0xbeefcafe))}, cs)

	assert.Len(l.Futures(), 1)
	assert.EqualValues(r, l.Futures()[0].Ref())
}
