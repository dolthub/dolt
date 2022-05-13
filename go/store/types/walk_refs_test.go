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
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestWalkRefs(t *testing.T) {
	runTest := func(nbf *NomsBinFormat, v Value, t *testing.T) {
		assert := assert.New(t)
		expected := hash.HashSlice{}
		v.walkRefs(nbf, func(r Ref) error {
			expected = append(expected, r.TargetHash())
			return nil
		})
		val, err := EncodeValue(v, nbf)
		require.NoError(t, err)
		err = walkRefs(val.Data(), nbf, func(r Ref) error {
			if assert.True(len(expected) > 0) {
				assert.Equal(expected[0], r.TargetHash())
				expected = expected[1:]
			}

			return nil
		})
		require.NoError(t, err)
		assert.Len(expected, 0)
	}

	t.Run("SingleRef", func(t *testing.T) {
		t.Parallel()
		t.Run("Typed", func(t *testing.T) {
			vrw := newTestValueStore()
			s, err := NewStruct(vrw.Format(), "", StructData{"n": Float(1)})
			require.NoError(t, err)
			runTest(vrw.Format(), mustRef(NewRef(mustMap(NewMap(context.Background(), vrw, s, Float(2))), vrw.Format())), t)
		})
		t.Run("OfValue", func(t *testing.T) {
			runTest(Format_Default, mustValue(ToRefOfValue(mustRef(NewRef(Bool(false), Format_Default)), Format_Default)), t)
		})
	})

	t.Run("Struct", func(t *testing.T) {
		t.Parallel()
		data := StructData{
			"ref": mustRef(NewRef(Bool(false), Format_Default)),
			"num": Float(42),
		}
		st, err := NewStruct(Format_Default, "nom", data)
		require.NoError(t, err)
		runTest(Format_Default, st, t)
	})

	// must return a slice with an even number of elements
	newValueSlice := func(nbf *NomsBinFormat, r *rand.Rand) ValueSlice {
		vs := make(ValueSlice, 256)
		for i := range vs {
			var err error
			vs[i], err = NewStruct(nbf, "", StructData{"n": Float(r.Uint64())})
			require.NoError(t, err)
		}
		return vs
	}

	t.Run("List", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			l, err := NewList(context.Background(), vrw, mustValue(vrw.WriteValue(context.Background(), Float(42))), mustValue(vrw.WriteValue(context.Background(), Float(0))))
			require.NoError(t, err)
			runTest(vrw.Format(), l, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			l, err := NewList(context.Background(), vrw, newValueSlice(vrw.Format(), r)...)
			require.NoError(t, err)
			for l.sequence.isLeaf() {
				l, err = l.Concat(context.Background(), mustList(NewList(context.Background(), vrw, newValueSlice(vrw.Format(), r)...)))
				require.NoError(t, err)
			}
			runTest(vrw.Format(), l, t)
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			s, err := NewSet(context.Background(), vrw, mustValue(vrw.WriteValue(context.Background(), Float(42))), mustValue(vrw.WriteValue(context.Background(), Float(0))))
			require.NoError(t, err)
			runTest(vrw.Format(), s, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			s, err := NewSet(context.Background(), vrw, newValueSlice(vrw.Format(), r)...)
			require.NoError(t, err)
			for s.isLeaf() {
				e := s.Edit()
				e, err = e.Insert(newValueSlice(vrw.Format(), r)...)
				require.NoError(t, err)
				s, err = e.Set(context.Background())
				require.NoError(t, err)
			}
			runTest(vrw.Format(), s, t)
		})
	})

	t.Run("Map", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			m, err := NewMap(context.Background(), vrw, mustValue(vrw.WriteValue(context.Background(), Float(42))), mustValue(vrw.WriteValue(context.Background(), Float(0))))
			require.NoError(t, err)
			runTest(vrw.Format(), m, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			m, err := NewMap(context.Background(), vrw, newValueSlice(vrw.Format(), r)...)
			require.NoError(t, err)
			for m.isLeaf() {
				e := m.Edit()
				vs := newValueSlice(vrw.Format(), r)
				for i := 0; i < len(vs); i += 2 {
					e = e.Set(vs[i], vs[i+1])
				}
				m, err = e.Map(context.Background())
				require.NoError(t, err)
			}
			runTest(vrw.Format(), m, t)
		})
	})

	t.Run("Blob", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		scratch := make([]byte, 1024)
		freshRandomBytes := func() io.Reader {
			r.Read(scratch)
			return bytes.NewReader(scratch)
		}
		b, err := NewBlob(context.Background(), vrw, freshRandomBytes())
		require.NoError(t, err)
		for b.sequence.isLeaf() {
			var err error
			b, err = b.Concat(context.Background(), mustBlob(NewBlob(context.Background(), vrw, freshRandomBytes())))
			require.NoError(t, err)
		}
		runTest(vrw.Format(), b, t)
	})
}
