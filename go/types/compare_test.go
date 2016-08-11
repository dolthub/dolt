// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

var prefix = []byte{0x01, 0x02, 0x03, 0x04}

func TestTotalOrdering(t *testing.T) {
	assert := assert.New(t)

	// values in increasing order. Some of these are compared by ref so changing the serialization might change the ordering.
	values := []Value{
		Bool(false), Bool(true),
		Number(-10), Number(0), Number(10),
		String("a"), String("b"), String("c"),

		// The order of these are done by the hash.
		NewSet(Number(0), Number(1), Number(2), Number(3)),
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
	comp := opCacheComparer{}

	b := append(prefix, byte(BoolKind), 0x00)
	n := append(prefix, byte(NumberKind), 0x00)
	s := append(prefix, byte(StringKind), 'a')

	assert.Equal(-1, comp.Compare(b, n))
	assert.Equal(-1, comp.Compare(b, s))
	assert.Equal(-1, comp.Compare(n, s))

	assert.Equal(1, comp.Compare(s, n))
	assert.Equal(1, comp.Compare(s, b))
	assert.Equal(1, comp.Compare(n, b))
}

func TestComparePrimitives(t *testing.T) {
	assert := assert.New(t)
	comp := opCacheComparer{}

	tru := encode(Bool(true))
	fls := encode(Bool(false))
	one := encode(Number(1))
	fortytwo := encode(Number(42))
	hey := encode(String("hey"))
	ya := encode(String("ya"))

	assert.Equal(-1, comp.Compare(fls, tru))
	assert.Equal(-1, comp.Compare(one, fortytwo))
	assert.Equal(-1, comp.Compare(hey, ya))

	assert.Equal(0, comp.Compare(tru, tru))
	assert.Equal(0, comp.Compare(one, one))
	assert.Equal(0, comp.Compare(hey, hey))

	assert.Equal(1, comp.Compare(tru, fls))
	assert.Equal(1, comp.Compare(fortytwo, one))
	assert.Equal(1, comp.Compare(ya, hey))
}

func TestCompareHashes(t *testing.T) {
	assert := assert.New(t)
	comp := opCacheComparer{}

	tru := encode(Bool(true))
	one := encode(Number(1))
	hey := encode(String("hey"))

	minHash := append(prefix, append([]byte{byte(BlobKind)}, bytes.Repeat([]byte{0}, hash.ByteLen)...)...)
	maxHash := append(prefix, append([]byte{byte(BlobKind)}, bytes.Repeat([]byte{0xff}, hash.ByteLen)...)...)
	almostMaxHash := append(prefix, append([]byte{byte(BlobKind)}, append(bytes.Repeat([]byte{0xff}, hash.ByteLen-1), 0xfe)...)...)

	assert.Equal(-1, comp.Compare(tru, minHash))
	assert.Equal(-1, comp.Compare(one, minHash))
	assert.Equal(-1, comp.Compare(hey, minHash))
	assert.Equal(-1, comp.Compare(minHash, almostMaxHash))
	assert.Equal(-1, comp.Compare(almostMaxHash, maxHash))

	assert.Equal(0, comp.Compare(minHash, minHash))
	assert.Equal(0, comp.Compare(almostMaxHash, almostMaxHash))
	assert.Equal(0, comp.Compare(maxHash, maxHash))

	assert.Equal(1, comp.Compare(minHash, tru))
	assert.Equal(1, comp.Compare(minHash, one))
	assert.Equal(1, comp.Compare(minHash, hey))
	assert.Equal(1, comp.Compare(almostMaxHash, tru))
	assert.Equal(1, comp.Compare(almostMaxHash, one))
	assert.Equal(1, comp.Compare(almostMaxHash, hey))
	assert.Equal(1, comp.Compare(maxHash, tru))
	assert.Equal(1, comp.Compare(maxHash, one))
	assert.Equal(1, comp.Compare(maxHash, hey))
	assert.Equal(1, comp.Compare(maxHash, almostMaxHash))
	assert.Equal(1, comp.Compare(almostMaxHash, minHash))

	almostMaxHash[5]++
	assert.Equal(1, comp.Compare(maxHash, almostMaxHash))

	almostMaxHash[0]++
	assert.Equal(-1, comp.Compare(maxHash, almostMaxHash))
}

func encode(v Value) []byte {
	w := &binaryNomsWriter{make([]byte, 128, 128), 0}
	newValueEncoder(w, nil).writeValue(v)
	return append(prefix, w.data()...)
}
