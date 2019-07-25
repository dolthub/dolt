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
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

func TestWalkRefs(t *testing.T) {
	runTest := func(v Value, t *testing.T) {
		assert := assert.New(t)
		expected := hash.HashSlice{}
		v.WalkRefs(Format_7_18, func(r Ref) error {
			expected = append(expected, r.TargetHash())
			return nil
		})
		val, err := EncodeValue(v, Format_7_18)
		assert.NoError(err)
		err = WalkRefs(val, Format_7_18, func(r Ref) error {
			if assert.True(len(expected) > 0) {
				assert.Equal(expected[0], r.TargetHash())
				expected = expected[1:]
			}

			return nil
		})
		assert.NoError(err)
		assert.Len(expected, 0)
	}

	t.Run("SingleRef", func(t *testing.T) {
		t.Parallel()
		t.Run("Typed", func(t *testing.T) {
			vrw := newTestValueStore()
			s := NewStruct(Format_7_18, "", StructData{"n": Float(1)})
			runTest(NewRef(NewMap(context.Background(), vrw, s, Float(2)), Format_7_18), t)
		})
		t.Run("OfValue", func(t *testing.T) {
			runTest(ToRefOfValue(NewRef(Bool(false), Format_7_18), Format_7_18), t)
		})
	})

	t.Run("Struct", func(t *testing.T) {
		t.Parallel()
		data := StructData{
			"ref": NewRef(Bool(false), Format_7_18),
			"num": Float(42),
		}
		runTest(NewStruct(Format_7_18, "nom", data), t)
	})

	// must return a slice with an even number of elements
	newValueSlice := func(r *rand.Rand) ValueSlice {
		vs := make(ValueSlice, 256)
		for i := range vs {
			vs[i] = NewStruct(Format_7_18, "", StructData{"n": Float(r.Uint64())})
		}
		return vs
	}

	t.Run("List", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			l := NewList(context.Background(), vrw, vrw.WriteValue(context.Background(), Float(42)), vrw.WriteValue(context.Background(), Float(0)))
			runTest(l, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			l := NewList(context.Background(), vrw, newValueSlice(r)...)
			for l.sequence.isLeaf() {
				l = l.Concat(context.Background(), NewList(context.Background(), vrw, newValueSlice(r)...))
			}
			runTest(l, t)
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			s := NewSet(context.Background(), vrw, vrw.WriteValue(context.Background(), Float(42)), vrw.WriteValue(context.Background(), Float(0)))
			runTest(s, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			s := NewSet(context.Background(), vrw, newValueSlice(r)...)
			for s.isLeaf() {
				e := s.Edit()
				e = e.Insert(newValueSlice(r)...)
				s = e.Set(context.Background())
			}
			runTest(s, t)
		})
	})

	t.Run("Map", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			m := NewMap(context.Background(), vrw, vrw.WriteValue(context.Background(), Float(42)), vrw.WriteValue(context.Background(), Float(0)))
			runTest(m, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			m := NewMap(context.Background(), vrw, newValueSlice(r)...)
			for m.isLeaf() {
				e := m.Edit()
				vs := newValueSlice(r)
				for i := 0; i < len(vs); i += 2 {
					e = e.Set(vs[i], vs[i+1])
				}
				m = e.Map(context.Background())
			}
			runTest(m, t)
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
		b := NewBlob(context.Background(), vrw, freshRandomBytes())
		for b.sequence.isLeaf() {
			b = b.Concat(context.Background(), NewBlob(context.Background(), vrw, freshRandomBytes()))
		}
		runTest(b, t)
	})
}
