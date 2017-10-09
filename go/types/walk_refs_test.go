// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

func TestWalkRefs(t *testing.T) {
	runTest := func(v Value, t *testing.T) {
		assert := assert.New(t)
		expected := hash.HashSlice{}
		v.WalkRefs(func(r Ref) {
			expected = append(expected, r.TargetHash())
		})
		WalkRefs(EncodeValue(v), func(r Ref) {
			if assert.True(len(expected) > 0) {
				assert.Equal(expected[0], r.TargetHash())
				expected = expected[1:]
			}
		})
		assert.Len(expected, 0)
	}

	t.Run("SingleRef", func(t *testing.T) {
		t.Parallel()
		t.Run("Typed", func(t *testing.T) {
			vrw := newTestValueStore()
			s := NewStruct("", StructData{"n": Number(1)})
			runTest(NewRef(NewMap(vrw, s, Number(2))), t)
		})
		t.Run("OfValue", func(t *testing.T) {
			runTest(ToRefOfValue(NewRef(Bool(false))), t)
		})
	})

	t.Run("Struct", func(t *testing.T) {
		t.Parallel()
		data := StructData{
			"ref": NewRef(Bool(false)),
			"num": Number(42),
		}
		runTest(NewStruct("nom", data), t)
	})

	// must return a slice with an even number of elements
	newValueSlice := func(r *rand.Rand) ValueSlice {
		vs := make(ValueSlice, 256)
		for i := range vs {
			vs[i] = NewStruct("", StructData{"n": Number(r.Uint64())})
		}
		return vs
	}

	t.Run("List", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			l := NewList(vrw, vrw.WriteValue(Number(42)), vrw.WriteValue(Number(0)))
			runTest(l, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			l := NewList(vrw, newValueSlice(r)...)
			for l.sequence.isLeaf() {
				l = l.Concat(NewList(vrw, newValueSlice(r)...))
			}
			runTest(l, t)
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			s := NewSet(vrw, vrw.WriteValue(Number(42)), vrw.WriteValue(Number(0)))
			runTest(s, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			s := NewSet(vrw, newValueSlice(r)...)
			for s.isLeaf() {
				e := s.Edit()
				e = e.Insert(newValueSlice(r)...)
				s = e.Set()
			}
			runTest(s, t)
		})
	})

	t.Run("Map", func(t *testing.T) {
		t.Parallel()
		vrw := newTestValueStore()
		r := rand.New(rand.NewSource(0))

		t.Run("OfRefs", func(t *testing.T) {
			m := NewMap(vrw, vrw.WriteValue(Number(42)), vrw.WriteValue(Number(0)))
			runTest(m, t)
		})

		t.Run("Chunked", func(t *testing.T) {
			m := NewMap(vrw, newValueSlice(r)...)
			for m.isLeaf() {
				e := m.Edit()
				vs := newValueSlice(r)
				for i := 0; i < len(vs); i += 2 {
					e = e.Set(vs[i], vs[i+1])
				}
				m = e.Map()
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
		b := NewBlob(vrw, freshRandomBytes())
		for b.sequence.isLeaf() {
			b = b.Concat(NewBlob(vrw, freshRandomBytes()))
		}
		runTest(b, t)
	})
}
