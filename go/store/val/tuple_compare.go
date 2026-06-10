// Copyright 2021 Dolthub, Inc.
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

package val

import (
	"bytes"
	"context"
)

// TupleComparator compares Tuples.
type TupleComparator interface {
	// Compare compares pairs of Tuples.
	Compare(ctx context.Context, left, right Tuple, desc *TupleDesc) (int, error)

	// CompareValues compares pairs of values. The index should match the index used to retrieve the type.
	CompareValues(ctx context.Context, index int, left, right []byte, typ Type) (int, error)

	// Prefix returns a TupleComparator for the first n types.
	Prefix(n int) TupleComparator

	// Suffix returns a TupleComparator for the last n types.
	Suffix(n int) TupleComparator

	// Validated returns a new TupleComparator that is valid against the given slice of types. Panics f a valid
	// TupleComparator cannot be returned.
	Validated(types []Type) TupleComparator

	// WithValueStore returns a copy of this TupleComparator that uses |vs| when comparing values that require
	// loading content from a content-addressed store (e.g. adaptive-encoded TEXT/BLOB values).
	WithValueStore(vs ValueStore) TupleComparator
}

type DefaultTupleComparator struct {
	vs ValueStore
}

var _ TupleComparator = &DefaultTupleComparator{}

// Compare implements TupleComparator
func (d *DefaultTupleComparator) Compare(ctx context.Context, left, right Tuple, desc *TupleDesc) (cmp int, err error) {
	off := len(desc.fast)
	var start, stop ByteSize
	for i := 0; i < off; i++ {
		stop = desc.fast[i]
		cmp, err = compare(ctx, desc.Types[i], left[start:stop], right[start:stop], d.vs)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
		start = stop
	}

	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp, err = compare(ctx, typ, left.GetField(j), right.GetField(j), d.vs)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return
}

// CompareValues implements TupleComparator
func (d *DefaultTupleComparator) CompareValues(ctx context.Context, index int, left, right []byte, typ Type) (cmp int, err error) {
	return compare(ctx, typ, left, right, d.vs)
}

// Prefix implements TupleComparator
func (d *DefaultTupleComparator) Prefix(n int) TupleComparator {
	return d
}

// Suffix implements TupleComparator
func (d *DefaultTupleComparator) Suffix(n int) TupleComparator {
	return d
}

// Validated implements TupleComparator
func (d *DefaultTupleComparator) Validated(types []Type) TupleComparator {
	return d
}

// WithValueStore implements TupleComparator
func (d *DefaultTupleComparator) WithValueStore(vs ValueStore) TupleComparator {
	return &DefaultTupleComparator{vs: vs}
}

func compare(ctx context.Context, typ Type, left, right []byte, vs ValueStore) (int, error) {
	// order NULLs first
	if left == nil || right == nil {
		if bytes.Equal(left, right) {
			return 0, nil
		} else if left == nil {
			return -1, nil
		} else {
			return 1, nil
		}
	}

	switch typ.Enc {
	case Int8Enc:
		return compareInt8(readInt8(left), readInt8(right)), nil
	case Uint8Enc:
		return compareUint8(readUint8(left), readUint8(right)), nil
	case Int16Enc:
		return compareInt16(readInt16(left), readInt16(right)), nil
	case Uint16Enc:
		return compareUint16(ReadUint16(left), ReadUint16(right)), nil
	case Int32Enc:
		return compareInt32(readInt32(left), readInt32(right)), nil
	case Uint32Enc:
		return compareUint32(ReadUint32(left), ReadUint32(right)), nil
	case Int64Enc:
		return compareInt64(readInt64(left), readInt64(right)), nil
	case Uint64Enc:
		return compareUint64(readUint64(left), readUint64(right)), nil
	case Float32Enc:
		return compareFloat32(readFloat32(left), readFloat32(right)), nil
	case Float64Enc:
		return compareFloat64(readFloat64(left), readFloat64(right)), nil
	case Bit64Enc:
		return compareBit64(readBit64(left), readBit64(right)), nil
	case DecimalEnc:
		return compareDecimal(readDecimal(left), readDecimal(right)), nil
	case YearEnc:
		return compareYear(readYear(left), readYear(right)), nil
	case DateEnc:
		return compareDate(readDate(left), readDate(right)), nil
	case TimeEnc:
		return compareTime(readTime(left), readTime(right)), nil
	case DatetimeEnc:
		return compareDatetime(readDatetime(left), readDatetime(right)), nil
	case EnumEnc:
		return compareEnum(readEnum(left), readEnum(right)), nil
	case SetEnc:
		return compareSet(readSet(left), readSet(right)), nil
	case StringEnc:
		return compareString(readString(left), readString(right)), nil
	case ByteStringEnc:
		return compareByteString(readByteString(left), readByteString(right)), nil
	case Hash128Enc:
		return compareHash128(readHash128(left), readHash128(right)), nil
	case GeomAddrEnc:
		return compareAddr(readAddr(left), readAddr(right)), nil
	case BytesAddrEnc:
		return compareAddr(readAddr(left), readAddr(right)), nil
	case CommitAddrEnc:
		return compareAddr(readAddr(left), readAddr(right)), nil
	case JSONAddrEnc:
		return compareAddr(readAddr(left), readAddr(right)), nil
	case StringAddrEnc:
		return compareAddr(readAddr(left), readAddr(right)), nil
	case CellEnc:
		return compareCell(readCell(left), readCell(right)), nil
	case BytesAdaptiveEnc, StringAdaptiveEnc, GeomAdaptiveEnc, JsonAdaptiveEnc:
		return vs.CompareAdaptive(ctx, left, right, typ.Enc)
	default:
		panic("unknown encoding")
	}
}
