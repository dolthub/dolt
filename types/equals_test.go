package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r1 := UInt16(1).Ref()
	r2 := UInt16(2).Ref()

	values := []func() Value{
		func() Value { return nil },
		func() Value { return Bool(false) },
		func() Value { return Bool(true) },
		func() Value { return Int8(0) },
		func() Value { return Int8(1) },
		func() Value { return Int8(-1) },
		func() Value { return Int16(0) },
		func() Value { return Int16(1) },
		func() Value { return Int16(-1) },
		func() Value { return Int32(0) },
		func() Value { return Int32(1) },
		func() Value { return Int32(-1) },
		func() Value { return Int64(0) },
		func() Value { return Int64(1) },
		func() Value { return Int64(-1) },
		func() Value { return UInt8(0) },
		func() Value { return UInt8(1) },
		func() Value { return UInt16(0) },
		func() Value { return UInt16(1) },
		func() Value { return UInt32(0) },
		func() Value { return UInt32(1) },
		func() Value { return UInt64(0) },
		func() Value { return UInt64(1) },
		func() Value { return Float32(0) },
		func() Value { return Float32(-1) },
		func() Value { return Float32(1) },
		func() Value { return Float64(0) },
		func() Value { return Float64(-1) },
		func() Value { return Float64(1) },
		func() Value { return NewString("") },
		func() Value { return NewString("hi") },
		func() Value { return NewString("bye") },
		func() Value {
			return NewMemoryBlob(&bytes.Buffer{})
		},
		func() Value {
			return NewMemoryBlob(bytes.NewBufferString("hi"))
		},
		func() Value {
			return NewMemoryBlob(bytes.NewBufferString("bye"))
		},
		func() Value {
			ms := chunks.NewMemoryStore()
			b1 := NewBlob(bytes.NewBufferString("hi"), ms)
			b2 := NewBlob(bytes.NewBufferString("bye"), ms)
			return newCompoundBlob([]metaTuple{{WriteValue(b1, ms), UInt64(uint64(2))}, {WriteValue(b2, ms), UInt64(uint64(5))}}, ms)
		},
		func() Value { return NewList(cs) },
		func() Value { return NewList(cs, NewString("foo")) },
		func() Value { return NewList(cs, NewString("bar")) },
		func() Value { return NewMap(cs) },
		func() Value { return NewMap(cs, NewString("a"), NewString("a")) },
		func() Value { return NewSet(cs) },
		func() Value { return NewSet(cs, NewString("hi")) },

		func() Value { return MakePrimitiveType(BoolKind) },
		func() Value { return MakePrimitiveType(StringKind) },
		func() Value { return MakeStructType("a", []Field{}, Choices{}) },
		func() Value { return MakeStructType("b", []Field{}, Choices{}) },
		func() Value { return MakeEnumType("E", "a", "b") },
		func() Value { return MakeEnumType("E", "a", "b", "c") },
		func() Value { return MakeCompoundType(ListKind, MakePrimitiveType(UInt64Kind)) },
		func() Value { return MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)) },
		func() Value { return MakeCompoundType(SetKind, MakePrimitiveType(UInt32Kind)) },
		func() Value { return MakeCompoundType(SetKind, MakePrimitiveType(Int32Kind)) },
		func() Value { return MakeCompoundType(RefKind, MakePrimitiveType(UInt16Kind)) },
		func() Value { return MakeCompoundType(RefKind, MakePrimitiveType(Int16Kind)) },
		func() Value {
			return MakeCompoundType(MapKind, MakePrimitiveType(UInt8Kind), MakePrimitiveType(ValueKind))
		},
		func() Value {
			return MakeCompoundType(MapKind, MakePrimitiveType(Int8Kind), MakePrimitiveType(ValueKind))
		},
		func() Value { return MakeType(r1, 0) },
		func() Value { return MakeType(r1, 1) },
		func() Value { return MakeType(r2, 0) },
		func() Value { return MakeUnresolvedType("ns", "a") },
		func() Value { return MakeUnresolvedType("ns", "b") },
		func() Value { return MakeUnresolvedType("ns2", "a") },
	}

	for i, f1 := range values {
		for j, f2 := range values {
			if f1() == nil {
				continue
			}
			if i == j {
				assert.True(f1().Equals(f2()))
			} else {
				assert.False(f1().Equals(f2()))
			}
		}
		v := f1()
		if v != nil {
			r := NewRef(v.Ref())
			assert.False(r.Equals(v))
			assert.False(v.Equals(r))
		}
	}
}
