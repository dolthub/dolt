// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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
			return NewBlob(context.Background(), vrw, &bytes.Buffer{})
		},
		func() Value {
			return NewBlob(context.Background(), vrw, bytes.NewBufferString("hi"))
		},
		func() Value {
			return NewBlob(context.Background(), vrw, bytes.NewBufferString("bye"))
		},
		func() Value {
			b1 := NewBlob(context.Background(), vrw, bytes.NewBufferString("hi"))
			b2 := NewBlob(context.Background(), vrw, bytes.NewBufferString("bye"))
			return newBlob(newBlobMetaSequence(1, []metaTuple{
				newMetaTuple(NewRef(b1, Format_7_18), orderedKeyFromInt(2, Format_7_18), 2),
				newMetaTuple(NewRef(b2, Format_7_18), orderedKeyFromInt(5, Format_7_18), 5),
			}, vrw))
		},
		func() Value { return NewList(context.Background(), vrw) },
		func() Value { return NewList(context.Background(), vrw, String("foo")) },
		func() Value { return NewList(context.Background(), vrw, String("bar")) },
		func() Value { return NewMap(context.Background(), vrw) },
		func() Value { return NewMap(context.Background(), vrw, String("a"), String("a")) },
		func() Value { return NewSet(context.Background(), vrw) },
		func() Value { return NewSet(context.Background(), vrw, String("hi")) },

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
				assert.True(f1().Equals(f2()))
			} else {
				assert.False(f1().Equals(f2()))
			}
		}
		v := f1()
		if v != nil {
			r := NewRef(v, Format_7_18)
			assert.False(r.Equals(v))
			assert.False(v.Equals(r))
		}
	}
}
