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
	"fmt"
	"strconv"
	"strings"
)

type TupleDesc struct {
	Types []Type

	// Under certain conditions, Tuple comparisons can be
	// optimized by directly comparing Tuples as byte slices,
	// rather than accessing and deserializing each field.
	// See definition of rawCmp for more information.
	raw rawCmp
}

func NewTupleDescriptor(types ...Type) (td TupleDesc) {
	if len(types) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}

	for _, typ := range types {
		if typ.Enc == NullEnc {
			panic("invalid encoding")
		}
	}

	td.Types = types
	td.raw = maybeGetRawComparison(types...)
	return
}

type Comparison int

const (
	GreaterCmp Comparison = 1
	EqualCmp   Comparison = 0
	LesserCmp  Comparison = -1
)

func (td TupleDesc) Compare(left, right Tuple) (cmp Comparison) {
	if td.raw != nil {
		return compareRaw(left, right, td.raw)
	}

	for i, typ := range td.Types {
		cmp = Comparison(compare(typ, left.GetField(i), right.GetField(i)))
		if cmp != EqualCmp {
			break
		}
	}

	return
}

func (td TupleDesc) Count() int {
	return len(td.Types)
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

func (td TupleDesc) GetString(i int, tup Tuple) string {
	td.expectEncoding(i, StringEnc)
	return readString(tup.GetField(i), td.Types[i].Coll)
}

func (td TupleDesc) GetBytes(i int, tup Tuple) []byte {
	td.expectEncoding(i, BytesEnc)
	return readBytes(tup.GetField(i), td.Types[i].Coll)
}

func (td TupleDesc) GetField(i int, tup Tuple) interface{} {
	switch td.Types[i].Enc {
	case Int8Enc:
		return td.GetInt8(i, tup)
	case Uint8Enc:
		return td.GetUint8(i, tup)
	case Int16Enc:
		return td.GetInt16(i, tup)
	case Uint16Enc:
		return td.GetUint16(i, tup)
	case Int24Enc:
		panic("24 bit")
	case Uint24Enc:
		panic("24 bit")
	case Int32Enc:
		return td.GetInt32(i, tup)
	case Uint32Enc:
		return td.GetUint32(i, tup)
	case Int64Enc:
		return td.GetInt64(i, tup)
	case Uint64Enc:
		return td.GetUint64(i, tup)
	case Float32Enc:
		return td.GetFloat32(i, tup)
	case Float64Enc:
		return td.GetFloat64(i, tup)
	case StringEnc:
		return td.GetString(i, tup)
	case BytesEnc:
		return td.GetBytes(i, tup)
	default:
		panic("unknown encoding")
	}
}

func (td TupleDesc) expectEncoding(i int, encodings ...Encoding) {
	for _, enc := range encodings {
		if enc == td.Types[i].Enc {
			return
		}
	}
	panic("incorrect value encoding")
}

func (td TupleDesc) Format(tup Tuple) string {
	var sb strings.Builder
	sb.WriteString("[ ")

	seenOne := false
	for i, typ := range td.Types {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true

		switch typ.Enc {
		case Int8Enc:
			v := td.GetInt8(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint8Enc:
			v := td.GetUint8(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int16Enc:
			v := td.GetInt16(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint16Enc:
			v := td.GetUint16(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int24Enc:
			panic("24 bit")
		case Uint24Enc:
			panic("24 bit")
		case Int32Enc:
			v := td.GetInt32(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint32Enc:
			v := td.GetUint32(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int64Enc:
			v := td.GetInt64(i, tup)
			sb.WriteString(strconv.FormatInt(v, 10))
		case Uint64Enc:
			v := td.GetUint64(i, tup)
			sb.WriteString(strconv.FormatUint(v, 10))
		case Float32Enc:
			v := td.GetFloat32(i, tup)
			sb.WriteString(fmt.Sprintf("%f", v))
		case Float64Enc:
			v := td.GetFloat64(i, tup)
			sb.WriteString(fmt.Sprintf("%f", v))
		case StringEnc:
			v := td.GetString(i, tup)
			sb.WriteString(v)
		case BytesEnc:
			v := td.GetBytes(i, tup)
			sb.Write(v)
		default:
			panic("unknown encoding")
		}
	}
	sb.WriteString(" ]")
	return sb.String()
}
