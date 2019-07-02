// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	values := []func() Value{
		func() Value { return Bool(false) },
		func() Value { return Bool(true) },
		func() Value { return Float(0) },
		func() Value { return Float(-1) },
		func() Value { return Float(1) },
		func() Value { return String("") },
		func() Value { return String("hi") },
		func() Value { return String("bye") },
		func() Value {
			// TODO(binformat)
			return NewBlob(context.Background(), Format_7_18, vrw, &bytes.Buffer{})
		},
		func() Value {
			// TODO(binformat)
			return NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("hi"))
		},
		func() Value {
			// TODO(binformat)
			return NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("bye"))
		},
		func() Value {
			// TODO(binformat)
			b1 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("hi"))
			b2 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("bye"))
			return newBlob(newBlobMetaSequence(1, []metaTuple{
				newMetaTuple(Format_7_18, NewRef(b1, Format_7_18), orderedKeyFromInt(2, Format_7_18), 2),
				newMetaTuple(Format_7_18, NewRef(b2, Format_7_18), orderedKeyFromInt(5, Format_7_18), 5),
			}, Format_7_18, nil), Format_7_18)
		},
		// TODO(binformat)
		func() Value { return NewList(context.Background(), vrw) },
		func() Value { return NewList(context.Background(), vrw, String("foo")) },
		func() Value { return NewList(context.Background(), vrw, String("bar")) },
		func() Value { return NewMap(context.Background(), vrw) },
		func() Value { return NewMap(context.Background(), vrw, String("a"), String("a")) },
		func() Value { return NewSet(context.Background(), Format_7_18, vrw) },
		func() Value { return NewSet(context.Background(), Format_7_18, vrw, String("hi")) },

		func() Value { return BoolType },
		func() Value { return StringType },
		func() Value { return MakeStructType("a") },
		func() Value { return MakeStructType("b") },
		func() Value { return MakeListType(BoolType) },
		func() Value { return MakeListType(FloaTType) },
		func() Value { return MakeSetType(BoolType) },
		func() Value { return MakeSetType(FloaTType) },
		func() Value { return MakeRefType(BoolType) },
		func() Value { return MakeRefType(FloaTType) },
		func() Value {
			return MakeMapType(BoolType, ValueType)
		},
		func() Value {
			return MakeMapType(FloaTType, ValueType)
		},
	}

	for i, f1 := range values {
		for j, f2 := range values {
			if i == j {
				assert.True(f1().Equals(Format_7_18, f2()))
			} else {
				assert.False(f1().Equals(Format_7_18, f2()))
			}
		}
		v := f1()
		if v != nil {
			r := NewRef(v, Format_7_18)
			assert.False(r.Equals(Format_7_18, v))
			assert.False(v.Equals(Format_7_18, r))
		}
	}
}
