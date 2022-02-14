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
	"time"
)

type TupleDesc struct {
	Types []Type
	cmp   TupleComparator
}

type TupleComparator interface {
	Compare(left, right Tuple, desc TupleDesc) int
}

type defaultCompare struct{}

func (d defaultCompare) Compare(left, right Tuple, desc TupleDesc) (cmp int) {
	for i, typ := range desc.Types {
		cmp = compare(typ, left.GetField(i), right.GetField(i))
		if cmp != 0 {
			break
		}
	}
	return
}

var _ TupleComparator = defaultCompare{}

func NewTupleDescriptor(types ...Type) TupleDesc {
	return NewTupleDescriptorWithComparator(defaultCompare{}, types...)
}

// NewTupleDescriptor returns a TupleDesc from a slice of Types.
func NewTupleDescriptorWithComparator(cmp TupleComparator, types ...Type) (td TupleDesc) {
	if len(types) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}

	for _, typ := range types {
		if typ.Enc == NullEnc {
			panic("invalid encoding")
		}
	}

	td.Types = types
	td.cmp = cmp

	return
}

func TupleDescriptorPrefix(td TupleDesc, count int) TupleDesc {
	return NewTupleDescriptorWithComparator(td.cmp, td.Types[:count]...)
}

// Compare returns the Comaparison of |left| and |right|.
func (td TupleDesc) Compare(left, right Tuple) (cmp int) {
	return td.cmp.Compare(left, right, td)
}

// Count returns the number of fields in the TupleDesc.
func (td TupleDesc) Count() int {
	return len(td.Types)
}

// IsNull returns true if the ith field of the Tuple is NULL.
func (td TupleDesc) IsNull(i int, tup Tuple) bool {
	b := tup.GetField(i)
	return b == nil
}

// GetBool reads a bool from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBool(i int, tup Tuple) (v bool, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readBool(b), true
	}
	return
}

// GetInt8 reads an int8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt8(i int, tup Tuple) (v int8, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readInt8(b), true
	}
	return
}

// GetUint8 reads a uint8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint8(i int, tup Tuple) (v uint8, ok bool) {
	td.expectEncoding(i, Uint8Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readUint8(b), true
	}
	return
}

// GetInt16 reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt16(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, Int16Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readInt16(b), true
	}
	return
}

// GetUint16 reads a uint16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint16(i int, tup Tuple) (v uint16, ok bool) {
	td.expectEncoding(i, Uint16Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readUint16(b), true
	}
	return
}

// GetInt32 reads an int32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt32(i int, tup Tuple) (v int32, ok bool) {
	td.expectEncoding(i, Int32Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readInt32(b), true
	}
	return
}

// GetUint32 reads a uint32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint32(i int, tup Tuple) (v uint32, ok bool) {
	td.expectEncoding(i, Uint32Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readUint32(b), true
	}
	return
}

// GetInt64 reads an int64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt64(i int, tup Tuple) (v int64, ok bool) {
	td.expectEncoding(i, Int64Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readInt64(b), true
	}
	return
}

// GetUint64 reads a uint64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint64(i int, tup Tuple) (v uint64, ok bool) {
	td.expectEncoding(i, Uint64Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readUint64(b), true
	}
	return
}

// GetFloat32 reads a float32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat32(i int, tup Tuple) (v float32, ok bool) {
	td.expectEncoding(i, Float32Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readFloat32(b), true
	}
	return
}

// GetFloat64 reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat64(i int, tup Tuple) (v float64, ok bool) {
	td.expectEncoding(i, Float64Enc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readFloat64(b), true
	}
	return
}

// GetDecimal reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetDecimal(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, DecimalEnc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readString(b), true
	}
	return
}

// GetTimestamp reads a time.Time from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetTimestamp(i int, tup Tuple) (v time.Time, ok bool) {
	td.expectEncoding(i, TimestampEnc, DateEnc, DatetimeEnc, YearEnc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readTimestamp(b), true
	}
	return
}

// GetSqlTime reads a string encoded Time value from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetSqlTime(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, TimeEnc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readString(b), true
	}
	return
}

// GetInt16 reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetYear(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, YearEnc)
	b := tup.GetField(i)
	if b != nil {
		v, ok = readInt16(b), true
	}
	return
}

// GetString reads a string from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetString(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, StringEnc)
	b := tup.GetField(i)
	if b != nil {
		v = readString(b)
		ok = true
	}
	return
}

// GetBytes reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBytes(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, BytesEnc)
	b := tup.GetField(i)
	if b != nil {
		v = readBytes(b)
		ok = true
	}
	return
}

// GetJSON reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetJSON(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, JSONEnc)
	b := tup.GetField(i)
	if b != nil {
		v = readBytes(b)
		ok = true
	}
	return
}

// GetBytes reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetGeometry(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, GeometryEnc)
	b := tup.GetField(i)
	if b != nil {
		v = readBytes(b)
		ok = true
	}
	return
}

func (td TupleDesc) expectEncoding(i int, encodings ...Encoding) {
	for _, enc := range encodings {
		if enc == td.Types[i].Enc {
			return
		}
	}
	panic("incorrect value encoding")
}

// Format prints a Tuple as a string.
func (td TupleDesc) Format(tup Tuple) string {
	if tup == nil || tup.Count() == 0 {
		return "( )"
	}

	var sb strings.Builder
	sb.WriteString("( ")

	seenOne := false
	for i, typ := range td.Types {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true

		if td.IsNull(i, tup) {
			sb.WriteString("NULL")
			continue
		}

		// todo(andy): complete cases
		switch typ.Enc {
		case Int8Enc:
			v, _ := td.GetInt8(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint8Enc:
			v, _ := td.GetUint8(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int16Enc:
			v, _ := td.GetInt16(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint16Enc:
			v, _ := td.GetUint16(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int32Enc:
			v, _ := td.GetInt32(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Uint32Enc:
			v, _ := td.GetUint32(i, tup)
			sb.WriteString(strconv.Itoa(int(v)))
		case Int64Enc:
			v, _ := td.GetInt64(i, tup)
			sb.WriteString(strconv.FormatInt(v, 10))
		case Uint64Enc:
			v, _ := td.GetUint64(i, tup)
			sb.WriteString(strconv.FormatUint(v, 10))
		case Float32Enc:
			v, _ := td.GetFloat32(i, tup)
			sb.WriteString(fmt.Sprintf("%f", v))
		case Float64Enc:
			v, _ := td.GetFloat64(i, tup)
			sb.WriteString(fmt.Sprintf("%f", v))
		case StringEnc:
			v, _ := td.GetString(i, tup)
			sb.WriteString(v)
		case BytesEnc:
			v, _ := td.GetBytes(i, tup)
			sb.Write(v)
		default:
			sb.Write(tup.GetField(i))
		}
	}
	sb.WriteString(" )")
	return sb.String()
}
