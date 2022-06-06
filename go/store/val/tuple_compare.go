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

// TupleComparator compares Tuples.
type TupleComparator interface {
	// Compare compares pairs of Tuples.
	Compare(left, right Tuple, desc TupleDesc) int

	// CompareValues compares pairs of values.
	CompareValues(left, right []byte, typ Type) int
}

type defaultCompare struct{}

var _ TupleComparator = defaultCompare{}

// Compare implements TupleComparator
func (d defaultCompare) Compare(left, right Tuple, desc TupleDesc) (cmp int) {
	for i := range desc.fast {
		start, stop := desc.fast[i][0], desc.fast[i][1]
		cmp = compare(desc.Types[i], left[start:stop], right[start:stop])
		if cmp != 0 {
			return cmp
		}
	}

	off := len(desc.fast)
	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp = compare(typ, left.GetField(j), right.GetField(j))
		if cmp != 0 {
			return cmp
		}
	}
	return
}

// CompareValues implements TupleComparator
func (d defaultCompare) CompareValues(left, right []byte, typ Type) (cmp int) {
	return compare(typ, left, right)
}

func compare(typ Type, left, right []byte) int {
	// order NULLs last
	if left == nil {
		if right == nil {
			return 0
		} else {
			return 1
		}
	} else if right == nil {
		if left == nil {
			return 0
		} else {
			return -1
		}
	}

	switch typ.Enc {
	case Int8Enc:
		return compareInt8(readInt8(left), readInt8(right))
	case Uint8Enc:
		return compareUint8(readUint8(left), readUint8(right))
	case Int16Enc:
		return compareInt16(readInt16(left), readInt16(right))
	case Uint16Enc:
		return compareUint16(readUint16(left), readUint16(right))
	case Int32Enc:
		return compareInt32(readInt32(left), readInt32(right))
	case Uint32Enc:
		return compareUint32(readUint32(left), readUint32(right))
	case Int64Enc:
		return compareInt64(readInt64(left), readInt64(right))
	case Uint64Enc:
		return compareUint64(readUint64(left), readUint64(right))
	case Float32Enc:
		return compareFloat32(readFloat32(left), readFloat32(right))
	case Float64Enc:
		return compareFloat64(readFloat64(left), readFloat64(right))
	case Bit64Enc:
		return compareBit64(readBit64(left), readBit64(right))
	case DecimalEnc:
		return compareDecimal(readDecimal(left), readDecimal(right))
	case YearEnc:
		return compareYear(readYear(left), readYear(right))
	case DateEnc:
		return compareDate(readDate(left), readDate(right))
	case TimeEnc:
		return compareTime(readTime(left), readTime(right))
	case DatetimeEnc:
		return compareDatetime(readDatetime(left), readDatetime(right))
	case EnumEnc:
		return compareEnum(readEnum(left), readEnum(right))
	case SetEnc:
		return compareSet(readSet(left), readSet(right))
	case StringEnc:
		return compareString(readString(left), readString(right))
	case ByteStringEnc:
		return compareByteString(readByteString(left), readByteString(right))
	case Hash128Enc:
		return compareHash128(readHash128(left), readHash128(right))
	default:
		panic("unknown encoding")
	}
}
