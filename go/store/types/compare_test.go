// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

const uint32Size = 4

func compareEncodedKeys(a, b []byte) int {
	if compared, res := compareEmpties(a, b); compared {
		return res
	}

	// keys are encoded as either values:
	//   nomsKind(1-byte) + serialized len(4-bytes) + serialized value(n-bytes)
	// or digests:
	//   nomsKind(1-byte) + digest(hash.Bytelen-bytes)
	splitAfterFirstKey := func(bs []byte) ([]byte, []byte) {
		keyLen := 1 + hash.ByteLen
		if isKindOrderedByValue(NomsKind(bs[0])) {
			l := int(binary.BigEndian.Uint32(bs[1:5]))
			keyLen = 1 + uint32Size + l
		}
		return bs[:keyLen], bs[keyLen:]
	}

	// a[0] and b[0] represent NomsKind of leafNode being operated on
	// a[1] and b[1] are the number of keys encoded in this byte slice
	numAGraphKeys, numBGraphKeys := a[1], b[1]
	minNumKeys := minByte(numAGraphKeys, numBGraphKeys)

	a, b = a[2:], b[2:]
	cres := 0
	for pos := 0; pos < int(minNumKeys) && cres == 0; pos++ {
		aKey, aRest := splitAfterFirstKey(a)
		bKey, bRest := splitAfterFirstKey(b)
		cres = compareEncodedKey(aKey, bKey)
		a, b = aRest, bRest
	}

	if cres == 0 {
		if numAGraphKeys < numBGraphKeys {
			return -1
		}
		if numAGraphKeys > numBGraphKeys {
			return 1
		}
	}
	return cres
}

// compareEncodedKey accepts two byte slices that each contain a number of
// encoded keys. It extracts the first key in each slice and returns the result
// of comparing them.
func compareEncodedKey(a, b []byte) int {
	// keys that are orderd by value are encoded as:
	//   NomsKind(1-byte) + length(4-bytes) + encoding(n-bytes)
	// keys that are not ordred by value are encoded as
	//   NomsKind(1-byte) + hash digest(20-bytes)

	aKind, bKind := NomsKind(a[0]), NomsKind(b[0])
	if !isKindOrderedByValue(aKind) && !isKindOrderedByValue(bKind) {
		a, b := a[1:], b[1:]
		d.PanicIfFalse(len(a) == hash.ByteLen && len(b) == hash.ByteLen)
		res := bytes.Compare(a, b)
		if res == 0 && aKind != bKind {
			d.Panic("Values of different kinds with the same hash. Whaa??")
		}
		return res
	}

	// Now, we know that at least one of a and b is ordered by value. So if the
	// kinds are different, we can sort just by comparing them.
	if res := compareKinds(aKind, bKind); res != 0 {
		return res
	}

	// Now we know that we are comparing two values that are both Bools, Numbers,
	// or Strings. Extract their length and create slices that just contain their
	// Noms encodings.
	lenA := binary.BigEndian.Uint32(a[1:5])
	lenB := binary.BigEndian.Uint32(b[1:5])

	// create a1, b1 slices that just contain encoding
	a1, b1 := a[1+uint32Size:1+uint32Size+lenA], b[1+uint32Size:1+uint32Size+lenB]

	return compareEncodedNomsValues(a1, b1)
}

// compareEncodedNomsValues compares two slices. Each slice contains a first
// byte that holds the nomsKind of the original key and an encoding for that key.
// This method relies on knowledge about how bytes are arranged in a Noms
// encoding and makes use of that for companing values efficiently.
func compareEncodedNomsValues(a, b []byte) int {
	if compared, res := compareEmpties(a, b); compared {
		return res
	}
	aKind, bKind := NomsKind(a[0]), NomsKind(b[0])
	if aKind != bKind {
		d.Panic("compareEncodedNomsValues, aKind: %v != bKind: %v", aKind, bKind)
	}

	switch aKind {
	case NullKind:
		// If both are of type null, then they are equal
		return 0
	case UUIDKind:
		return bytes.Compare(a, b)
	case BoolKind:
		return bytes.Compare(a, b)
	case IntKind:
		reader := binaryNomsReader{a[1:], 0}
		aNum := reader.readInt()
		reader.buff, reader.offset = b[1:], 0
		bNum := reader.readInt()
		if aNum == bNum {
			return 0
		}
		if aNum < bNum {
			return -1
		}
		return 1
	case UintKind:
		reader := binaryNomsReader{a[1:], 0}
		aNum := reader.readUint()
		reader.buff, reader.offset = b[1:], 0
		bNum := reader.readUint()
		if aNum == bNum {
			return 0
		}
		if aNum < bNum {
			return -1
		}
		return 1
	case FloatKind:
		reader := binaryNomsReader{a[1:], 0}
		aNum := reader.readFloat(Format_7_18)
		reader.buff, reader.offset = b[1:], 0
		bNum := reader.readFloat(Format_7_18)
		if aNum == bNum {
			return 0
		}
		if aNum < bNum {
			return -1
		}
		return 1
	case StringKind:
		// Skip past uvarint-encoded string length
		_, aCount := binary.Uvarint(a[1:])
		_, bCount := binary.Uvarint(b[1:])
		res := bytes.Compare(a[1+aCount:], b[1+bCount:])
		return res
	}
	panic("unreachable")
}

func compareEmpties(a, b []byte) (bool, int) {
	aLen, bLen := len(a), len(b)
	if aLen > 0 && bLen > 0 {
		return false, 0
	}
	if aLen == 0 {
		if bLen == 0 {
			return true, 0
		}
		return true, -1
	}
	return true, 1
}

func compareKinds(aKind, bKind NomsKind) (res int) {
	if aKind < bKind {
		res = -1
	} else if aKind > bKind {
		res = 1
	}
	return
}

func minByte(a, b byte) byte {
	if a < b {
		return a
	}
	return b
}

// encodeKeys() serializes a list of keys to the byte slice |bs|.
func encodeKeys(bs []byte, colId uint32, opKind NomsKind, keys []Value) ([]byte, []Value) {
	// All ldb keys start with a 4-byte collection id that serves as a namespace
	// that keeps them separate from other collections.
	idHolder := [4]byte{}
	idHolderSlice := idHolder[:4]
	binary.BigEndian.PutUint32(idHolderSlice, colId)
	bs = append(bs, idHolderSlice...)

	// bs[4] is a NomsKind value which represents the type of leaf
	//   collection being operated on (i.e. MapKind, SetKind, or ListKind)
	// bs[5] is a single uint8 value representing the number of keys
	//   encoded in the ldb key.
	bs = append(bs, byte(opKind), byte(len(keys)))

	valuesToEncode := ValueSlice{}
	for _, gk := range keys {
		bs = encodeGraphKey(bs, gk)
		if !isKindOrderedByValue(gk.Kind()) {
			valuesToEncode = append(valuesToEncode, gk)
		}
	}
	return bs, valuesToEncode
}

func encodeGraphKey(bs []byte, v Value) []byte {
	return encodeForGraph(bs, v, false)
}

func encodeForGraph(bs []byte, v Value, asValue bool) []byte {
	// Note: encToSlice() and append() will both grow the backing store of |bs|
	// as necessary. Always call them when writing to |bs|.
	if asValue || isKindOrderedByValue(v.Kind()) {
		// if we're encoding value, then put:
		// noms-kind(1-byte), serialization-len(4-bytes), serialization(n-bytes)
		buf := [initialBufferSize]byte{}
		uint32buf := [4]byte{}
		encodedVal := encToSlice(v, buf[:])
		binary.BigEndian.PutUint32(uint32buf[:], uint32(len(encodedVal)))
		bs = append(bs, uint8(v.Kind()))
		bs = append(bs, uint32buf[:]...)
		bs = append(bs, encodedVal...)
	} else {
		// if we're encoding hash values, we know the length, so we can leave that out
		bs = append(bs, uint8(v.Kind()))
		// TODO(binformat)
		h := v.Hash(Format_7_18)
		bs = append(bs, h[:]...)
	}
	return bs
}

// Note that, if 'v' are prolly trees, any in-memory child chunks will be written to vw at this time.
func encToSlice(v Value, initBuf []byte) []byte {
	// TODO: Are there enough calls to this that it's worth re-using a nomsWriter?
	w := &binaryNomsWriter{initBuf, 0}
	// TODO(binformat)
	v.writeTo(w, Format_7_18)
	return w.data()
}

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
				// TODO(binformat)
				x := vi.Less(Format_7_18, vj)
				assert.True(x)
			} else {
				// TODO(binformat)
				x := vi.Less(Format_7_18, vj)
				assert.False(x)
			}
		}
	}
}

func TestCompareDifferentPrimitiveTypes(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()
	defer vrw.Close()

	nums := ValueSlice{Float(1), Float(2), Float(3)}
	words := ValueSlice{String("k1"), String("v1")}

	// TODO(binformat)
	blob := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{1, 2, 3}))
	nList := NewList(context.Background(), Format_7_18, vrw, nums...)
	nMap := NewMap(context.Background(), Format_7_18, vrw, words...)
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
	vrw := newTestValueStore()
	defer vrw.Close()

	k1 := ValueSlice{String("one"), Float(3)}
	k2 := ValueSlice{String("one"), Float(5)}

	bs1 := [initialBufferSize]byte{}
	bs2 := [initialBufferSize]byte{}

	e1, _ := encodeKeys(bs1[:0], 0x01020304, MapKind, k1)
	e2, _ := encodeKeys(bs2[:0], 0x01020304, MapKind, k2)
	assert.Equal(-1, compareEncodedKeys(e1, e2))
}

func encode(v Value) []byte {
	w := &binaryNomsWriter{make([]byte, 128), 0}
	// TODO(binformat)
	v.writeTo(w, Format_7_18)
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
