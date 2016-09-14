// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type opCacheComparer struct{}

func (opCacheComparer) Compare(a, b []byte) int {
	if res := bytes.Compare(a[:uint32Size], b[:uint32Size]); res != 0 {
		return res
	}
	a, b = a[uint32Size:], b[uint32Size:]

	if compared, res := compareEmpties(a, b); compared {
		return res
	}
	aKind, bKind := NomsKind(a[0]), NomsKind(b[0])
	switch aKind {
	default:
		if bKind <= StringKind {
			return 1
		}
		a, b = a[1:], b[1:]
		d.PanicIfFalse(len(a) == hash.ByteLen && len(b) == hash.ByteLen)
		res := bytes.Compare(a, b)
		d.PanicIfFalse(res != 0 || aKind == bKind)
		return res
	case BoolKind:
		return bytes.Compare(a, b)
	case NumberKind:
		if res := compareKinds(aKind, bKind); res != 0 {
			return res
		}
		reader := binaryNomsReader{a[1:], 0}
		aNum := reader.readNumber()
		reader.buff, reader.offset = b[1:], 0
		bNum := reader.readNumber()
		if aNum == bNum {
			return 0
		}
		if aNum < bNum {
			return -1
		}
		return 1
	case StringKind:
		if bKind == StringKind {
			a, b = a[1+uint32Size:], b[1+uint32Size:]
		}
		return bytes.Compare(a, b)
	}
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

func (opCacheComparer) Name() string {
	return "noms.OpCacheComparator"
}

func (opCacheComparer) Successor(dst, b []byte) []byte {
	return nil
}

func (opCacheComparer) Separator(dst, a, b []byte) []byte {
	return nil
}
