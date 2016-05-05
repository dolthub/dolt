package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)

	values := []func() Value{
		func() Value { return nil },
		func() Value { return Bool(false) },
		func() Value { return Bool(true) },
		func() Value { return Number(0) },
		func() Value { return Number(-1) },
		func() Value { return Number(1) },
		func() Value { return NewString("") },
		func() Value { return NewString("hi") },
		func() Value { return NewString("bye") },
		func() Value {
			return NewBlob(&bytes.Buffer{})
		},
		func() Value {
			return NewBlob(bytes.NewBufferString("hi"))
		},
		func() Value {
			return NewBlob(bytes.NewBufferString("bye"))
		},
		func() Value {
			b1 := NewBlob(bytes.NewBufferString("hi"))
			b2 := NewBlob(bytes.NewBufferString("bye"))
			return newCompoundBlob([]metaTuple{
				newMetaTuple(Number(uint64(2)), b1, NewTypedRefFromValue(b1), 2),
				newMetaTuple(Number(uint64(5)), b2, NewTypedRefFromValue(b2), 5),
			}, nil)
		},
		func() Value { return NewList() },
		func() Value { return NewList(NewString("foo")) },
		func() Value { return NewList(NewString("bar")) },
		func() Value { return NewMap() },
		func() Value { return NewMap(NewString("a"), NewString("a")) },
		func() Value { return NewSet() },
		func() Value { return NewSet(NewString("hi")) },

		func() Value { return BoolType },
		func() Value { return StringType },
		func() Value { return MakeStructType("a", TypeMap{}) },
		func() Value { return MakeStructType("b", TypeMap{}) },
		func() Value { return MakeListType(BoolType) },
		func() Value { return MakeListType(NumberType) },
		func() Value { return MakeSetType(BoolType) },
		func() Value { return MakeSetType(NumberType) },
		func() Value { return MakeRefType(BoolType) },
		func() Value { return MakeRefType(NumberType) },
		func() Value {
			return MakeMapType(BoolType, ValueType)
		},
		func() Value {
			return MakeMapType(NumberType, ValueType)
		},
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
			r := NewTypedRefFromValue(v)
			assert.False(r.Equals(v))
			assert.False(v.Equals(r))
		}
	}
}
