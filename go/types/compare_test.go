// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

var prefix = []byte{0x01, 0x02, 0x03, 0x04}

func TestCompareTotalOrdering(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// values in increasing order. Some of these are compared by ref so changing the serialization might change the ordering.
	values := []Value{
		Bool(false), Bool(true),
		Float(-10), Float(0), Float(10),
		String("a"), String("b"), String("c"),

		// The order of these are done by the hash.
		NewSet(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3)),
		BoolType,

		// Value - values cannot be value
		// Cycle - values cannot be cycle
		// Union - values cannot be unions
	}

	for i, vi := range values {
		for j, vj := range values {
			if i == j {
				assert.True(vi.Equals(vj))
			} else if i < j {
				x := vi.Less(vj)
				assert.True(x)
			} else {
				x := vi.Less(vj)
				assert.False(x)
			}
		}
	}
}

func TestCompareEmpties(t *testing.T) {
	assert := assert.New(t)
	comp := opCacheComparer{}
	assert.Equal(-1, comp.Compare(prefix, append(prefix, 0xff)))
	assert.Equal(0, comp.Compare(prefix, prefix))
	assert.Equal(1, comp.Compare(append(prefix, 0xff), prefix))
}

func TestCompareDifferentPrimitiveTypes(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()
	defer vrw.Close()

	nums := ValueSlice{Float(1), Float(2), Float(3)}
	words := ValueSlice{String("k1"), String("v1")}

	blob := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{1, 2, 3}))
	nList := NewList(context.Background(), vrw, nums...)
	nMap := NewMap(context.Background(), vrw, words...)
	nRef := NewRef(blob)
	nSet := NewSet(context.Background(), vrw, nums...)
	nStruct := NewStruct("teststruct", map[string]Value{"f1": Float(1)})

	vals := ValueSlice{Bool(true), Float(19), String("hellow"), blob, nList, nMap, nRef, nSet, nStruct}
	sort.Sort(vals)

	for i, v1 := range vals {
		for j, v2 := range vals {
			iBytes := [1024]byte{}
			jBytes := [1024]byte{}
			res := compareEncodedKey(encodeGraphKey(iBytes[:0], v1), encodeGraphKey(jBytes[:0], v2))
			expectedRes := compareInts(i, j)

			assert.Equal(expectedRes, res, "%d:%d", i, j)
		}
	}
}

func TestComparePrimitives(t *testing.T) {
	assert := assert.New(t)

	bools := []Bool{false, true}
	for i, v1 := range bools {
		for j, v2 := range bools {
			res := compareEncodedNomsValues(encode(v1), encode(v2))
			assert.Equal(compareInts(i, j), res)
		}
	}

	nums := []Float{-1111.29, -23, 0, 4.2345, 298}
	for i, v1 := range nums {
		for j, v2 := range nums {
			res := compareEncodedNomsValues(encode(v1), encode(v2))
			assert.Equal(compareInts(i, j), res)
		}
	}

	words := []String{"", "aaa", "another", "another1"}
	for i, v1 := range words {
		for j, v2 := range words {
			res := compareEncodedNomsValues(encode(v1), encode(v2))
			assert.Equal(compareInts(i, j), res)
		}
	}
}

func TestCompareEncodedKeys(t *testing.T) {
	assert := assert.New(t)
	comp := opCacheComparer{}
	vrw := newTestValueStore()
	defer vrw.Close()

	k1 := ValueSlice{String("one"), Float(3)}
	k2 := ValueSlice{String("one"), Float(5)}

	bs1 := [initialBufferSize]byte{}
	bs2 := [initialBufferSize]byte{}

	e1, _ := encodeKeys(bs1[:0], 0x01020304, MapKind, k1)
	e2, _ := encodeKeys(bs2[:0], 0x01020304, MapKind, k2)
	assert.Equal(-1, comp.Compare(e1, e2))
}

func encode(v Value) []byte {
	w := &binaryNomsWriter{make([]byte, 128, 128), 0}
	v.writeTo(w)
	return w.data()
}

func compareInts(i, j int) (res int) {
	if i < j {
		res = -1
	} else if i > j {
		res = 1
	}
	return
}
