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
	"time"
)

type Type struct {
	Enc      Encoding
	Coll     Collation
	Nullable bool
}

type TupleDesc struct {
	types []Type

	rawCompare []int
}

func NewTupleDescriptor(types ...Type) (td TupleDesc) {
	return TupleDesc{types: types}
}

func NewRawTupleDescriptor(cmps []int, types ...Type) (td TupleDesc) {
	td = NewTupleDescriptor(types...)
	td.rawCompare = cmps
	return
}

func (td TupleDesc) count() int {
	return len(td.types)
}

func (td TupleDesc) Compare(left, right Tuple) (cmp Comparison) {
	if td.rawCompare != nil {
		var l, r byte
		for _, idx := range td.rawCompare {
			l, r = left[idx], right[idx]
			if l != r {
				break
			}
		}
		if l > r {
			return 1
		}
		if l < r {
			return -1
		}
		return 0
	}

	for i, typ := range td.types {
		cmp = compare(typ, left.GetField(i), right.GetField(i))
		if cmp != EqualCmp {
			break
		}
	}
	return
}

func (td TupleDesc) GetBool(i int, tup Tuple) bool {
	td.expectEncoding(i, Int8Enc)
	return readBool(tup.GetField(i))
}

func (td TupleDesc) GetInt8(i int, tup Tuple) int8 {
	td.expectEncoding(i, Int8Enc)
	return readInt8(tup.GetField(i))
}

func (td TupleDesc) GetUint8(i int, tup Tuple) uint8 {
	td.expectEncoding(i, Uint8Enc)
	return readUint8(tup.GetField(i))
}

func (td TupleDesc) GetInt16(i int, tup Tuple) int16 {
	td.expectEncoding(i, Int16Enc)
	return readInt16(tup.GetField(i))
}

func (td TupleDesc) GetUint16(i int, tup Tuple) uint16 {
	td.expectEncoding(i, Uint16Enc)
	return readUint16(tup.GetField(i))
}

func (td TupleDesc) GetInt32(i int, tup Tuple) int32 {
	td.expectEncoding(i, Int32Enc)
	return readInt32(tup.GetField(i))
}

func (td TupleDesc) GetUint32(i int, tup Tuple) uint32 {
	td.expectEncoding(i, Uint32Enc)
	return readUint32(tup.GetField(i))
}

func (td TupleDesc) GetInt64(i int, tup Tuple) int64 {
	td.expectEncoding(i, Int64Enc)
	return readInt64(tup.GetField(i))
}

func (td TupleDesc) GetUint64(i int, tup Tuple) uint64 {
	td.expectEncoding(i, Uint64Enc)
	return readUint64(tup.GetField(i))
}

func (td TupleDesc) GetFloat32(i int, tup Tuple) float32 {
	td.expectEncoding(i, Float32Enc)
	return readFloat32(tup.GetField(i))
}

func (td TupleDesc) GetFloat64(i int, tup Tuple) float64 {
	td.expectEncoding(i, Float64Enc)
	return readFloat64(tup.GetField(i))
}

func (td TupleDesc) GetTime(i int, tup Tuple) time.Time {
	td.expectEncoding(i, TimeEnc)
	return readTime(tup.GetField(i))
}

func (td TupleDesc) GetString(i int, tup Tuple) string {
	td.expectEncoding(i, StringEnc)
	return readString(tup.GetField(i))
}

func (td TupleDesc) GetBytes(i int, tup Tuple) []byte {
	td.expectEncoding(i, BytesEnc)
	return readBytes(tup.GetField(i))
}

func (td TupleDesc) expectEncoding(i int, encodings ...Encoding) {
	for _, enc := range encodings {
		if enc == td.types[i].Enc {
			return
		}
	}
	panic("incorrect value encoding")
}

type Collation uint16

const (
	ByteOrderCollation Collation = 0
)

type Comparison int

const (
	GreaterCmp Comparison = 1
	EqualCmp   Comparison = 0
	LesserCmp  Comparison = -1
)

func compare(typ Type, left, right []byte) Comparison {
	if typ.Coll != ByteOrderCollation {
		panic("unknown collation")
	}
	return Comparison(bytes.Compare(left, right))
}
