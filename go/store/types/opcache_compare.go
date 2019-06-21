// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"encoding/binary"

	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	"github.com/liquidata-inc/ld/dolt/go/store/go/hash"
)

type opCacheComparer struct{}

func (opCacheComparer) Compare(a, b []byte) int {
	if res := bytes.Compare(a[:uint32Size], b[:uint32Size]); res != 0 {
		return res
	}
	return compareEncodedKeys(a[uint32Size:], b[uint32Size:])
}

func (opCacheComparer) Name() string {
	return "noms.OpCacheComparator"
}

func (opCacheComparer) Successor(dst, b []byte) []byte {
	return nil
}

func (opCacheComparer) Separator(dst, a, b []byte) []byte {
	return nil
}

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
		aNum := reader.readFloat()
		reader.buff, reader.offset = b[1:], 0
		bNum := reader.readFloat()
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
