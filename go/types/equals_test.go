// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)

	values := []func() Value{
		func() Value { return Bool(false) },
		func() Value { return Bool(true) },
		func() Value { return Number(0) },
		func() Value { return Number(-1) },
		func() Value { return Number(1) },
		func() Value { return String("") },
		func() Value { return String("hi") },
		func() Value { return String("bye") },
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
			return newBlob(newBlobMetaSequence(1, []metaTuple{
				newMetaTuple(NewRef(b1), orderedKeyFromInt(2), 2, b1),
				newMetaTuple(NewRef(b2), orderedKeyFromInt(5), 5, b2),
			}, nil))
		},
		func() Value { return NewList() },
		func() Value { return NewList(String("foo")) },
		func() Value { return NewList(String("bar")) },
		func() Value { return NewMap() },
		func() Value { return NewMap(String("a"), String("a")) },
		func() Value { return NewSet() },
		func() Value { return NewSet(String("hi")) },

		func() Value { return BoolType },
		func() Value { return StringType },
		func() Value { return MakeStructType("a") },
		func() Value { return MakeStructType("b") },
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
			if i == j {
				assert.True(f1().Equals(f2()))
			} else {
				assert.False(f1().Equals(f2()))
			}
		}
		v := f1()
		if v != nil {
			r := NewRef(v)
			assert.False(r.Equals(v))
			assert.False(v.Equals(r))
		}
	}
}
