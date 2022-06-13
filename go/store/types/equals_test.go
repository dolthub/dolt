// Copyright 2019 Dolthub, Inc.
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
	"github.com/stretchr/testify/require"
)

func TestValueEquals(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	values := []func() (Value, error){
		func() (Value, error) { return Bool(false), nil },
		func() (Value, error) { return Bool(true), nil },
		func() (Value, error) { return Float(0), nil },
		func() (Value, error) { return Float(-1), nil },
		func() (Value, error) { return Float(1), nil },
		func() (Value, error) { return String(""), nil },
		func() (Value, error) { return String("hi"), nil },
		func() (Value, error) { return String("bye"), nil },
		func() (Value, error) {
			return NewBlob(context.Background(), vrw, &bytes.Buffer{})
		},
		func() (Value, error) {
			return NewBlob(context.Background(), vrw, bytes.NewBufferString("hi"))
		},
		func() (Value, error) {
			return NewBlob(context.Background(), vrw, bytes.NewBufferString("bye"))
		},
		func() (Value, error) {
			b1, err := NewBlob(context.Background(), vrw, bytes.NewBufferString("hi"))

			if err != nil {
				return nil, err
			}

			b2, err := NewBlob(context.Background(), vrw, bytes.NewBufferString("bye"))

			if err != nil {
				return nil, err
			}

			return newBlob(mustSeq(newBlobMetaSequence(1, []metaTuple{
				mustMetaTuple(newMetaTuple(mustRef(NewRef(b1, vrw.Format())), mustOrdKey(orderedKeyFromInt(2, vrw.Format())), 2)),
				mustMetaTuple(newMetaTuple(mustRef(NewRef(b2, vrw.Format())), mustOrdKey(orderedKeyFromInt(5, vrw.Format())), 5)),
			}, vrw))), nil
		},
		func() (Value, error) { return NewList(context.Background(), vrw) },
		func() (Value, error) { return NewList(context.Background(), vrw, String("foo")) },
		func() (Value, error) { return NewList(context.Background(), vrw, String("bar")) },
		func() (Value, error) { return NewMap(context.Background(), vrw) },
		func() (Value, error) { return NewMap(context.Background(), vrw, String("a"), String("a")) },
		func() (Value, error) { return NewSet(context.Background(), vrw) },
		func() (Value, error) { return NewSet(context.Background(), vrw, String("hi")) },

		func() (Value, error) { return PrimitiveTypeMap[BoolKind], nil },
		func() (Value, error) { return PrimitiveTypeMap[StringKind], nil },
		func() (Value, error) { return MakeStructType("a") },
		func() (Value, error) { return MakeStructType("b") },
		func() (Value, error) { return MakeListType(PrimitiveTypeMap[BoolKind]) },
		func() (Value, error) { return MakeListType(PrimitiveTypeMap[FloatKind]) },
		func() (Value, error) { return MakeSetType(PrimitiveTypeMap[BoolKind]) },
		func() (Value, error) { return MakeSetType(PrimitiveTypeMap[FloatKind]) },
		func() (Value, error) { return MakeRefType(PrimitiveTypeMap[BoolKind]) },
		func() (Value, error) { return MakeRefType(PrimitiveTypeMap[FloatKind]) },
		func() (Value, error) {
			return MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[ValueKind])
		},
		func() (Value, error) {
			return MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[ValueKind])
		},
	}

	for i, f1 := range values {
		for j, f2 := range values {
			v1, err := f1()
			require.NoError(t, err)
			v2, err := f2()
			require.NoError(t, err)
			assert.Equal(v1.Equals(v2), i == j)
		}

		v, err := f1()
		require.NoError(t, err)
		if v != nil {
			r, err := NewRef(v, vrw.Format())
			require.NoError(t, err)
			assert.False(r.Equals(v))
			assert.False(v.Equals(r))
		}
	}
}
