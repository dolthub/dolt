package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)

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
			v, _ := NewBlob(&bytes.Buffer{})
			return v
		},
		func() Value {
			v, _ := NewBlob(bytes.NewBufferString("hi"))
			return v
		},
		func() Value {
			v, _ := NewBlob(bytes.NewBufferString("bye"))
			return v
		},
		func() Value {
			b1, _ := NewBlob(bytes.NewBufferString("hi"))
			b2, _ := NewBlob(bytes.NewBufferString("bye"))
			return newCompoundBlob([]uint64{2, 5}, []Future{futureFromValue(b1), futureFromValue(b2)}, nil)
		},
		func() Value { return NewList() },
		func() Value { return NewList(NewString("foo")) },
		func() Value { return NewList(NewString("bar")) },
		func() Value { return NewMap() },
		func() Value { return NewMap(NewString("a"), NewString("a")) },
		func() Value { return NewSet() },
		func() Value { return NewSet(NewString("hi")) },

		func() Value { return MakePrimitiveTypeRef(BoolKind) },
		func() Value { return MakePrimitiveTypeRef(StringKind) },
		func() Value { return MakeStructTypeRef("a", []Field{}, Choices{}) },
		func() Value { return MakeStructTypeRef("b", []Field{}, Choices{}) },
		func() Value { return MakeEnumTypeRef("E", "a", "b") },
		func() Value { return MakeEnumTypeRef("E", "a", "b", "c") },
		func() Value { return MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(UInt64Kind)) },
		func() Value { return MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(Int64Kind)) },
		func() Value { return MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(UInt32Kind)) },
		func() Value { return MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(Int32Kind)) },
		func() Value { return MakeCompoundTypeRef(RefKind, MakePrimitiveTypeRef(UInt16Kind)) },
		func() Value { return MakeCompoundTypeRef(RefKind, MakePrimitiveTypeRef(Int16Kind)) },
		func() Value {
			return MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(UInt8Kind), MakePrimitiveTypeRef(ValueKind))
		},
		func() Value {
			return MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(Int8Kind), MakePrimitiveTypeRef(ValueKind))
		},
		func() Value { return MakeTypeRef(r1, 0) },
		func() Value { return MakeTypeRef(r1, 1) },
		func() Value { return MakeTypeRef(r2, 0) },
		func() Value { return MakeUnresolvedTypeRef("ns", "a") },
		func() Value { return MakeUnresolvedTypeRef("ns", "b") },
		func() Value { return MakeUnresolvedTypeRef("ns2", "a") },
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
