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

// TupleDesc describes a Tuple set.
// Data structures that contain Tuples and algorithms that process Tuples
// use a TupleDesc's types to interpret the fields of a Tuple.
type TupleDesc struct {
	Types []Type
	cmp   TupleComparator
	fast  fixedAccess
}

// NewTupleDescriptor makes a TupleDescriptor from |types|.
func NewTupleDescriptor(types ...Type) TupleDesc {
	return NewTupleDescriptorWithComparator(defaultCompare{}, types...)
}

// NewTupleDescriptorWithComparator returns a TupleDesc from a slice of Types.
func NewTupleDescriptorWithComparator(cmp TupleComparator, types ...Type) (td TupleDesc) {
	if len(types) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}

	for _, typ := range types {
		if typ.Enc == NullEnc {
			panic("invalid encoding")
		}
	}

	td = TupleDesc{
		Types: types,
		cmp:   cmp,
		fast:  makeFixedAccess(types),
	}

	return
}

type fixedAccess [][2]ByteSize

func makeFixedAccess(types []Type) (acc fixedAccess) {
	acc = make(fixedAccess, 0, len(types))

	off := ByteSize(0)
	for _, typ := range types {
		if typ.Nullable {
			break
		}
		sz, ok := sizeFromType(typ)
		if !ok {
			break
		}
		acc = append(acc, [2]ByteSize{off, off + sz})
		off += sz
	}
	return
}

// GetField returns the ith field of |tup|.
func (td TupleDesc) GetField(i int, tup Tuple) []byte {
	if i < len(td.fast) {
		start, stop := td.fast[i][0], td.fast[i][1]
		return tup[start:stop]
	}
	return tup.GetField(i)
}

// Compare compares |left| and |right|.
func (td TupleDesc) Compare(left, right Tuple) (cmp int) {
	return td.cmp.Compare(left, right, td)
}

// CompareField compares |value| with the ith field of |tup|.
func (td TupleDesc) CompareField(value []byte, i int, tup Tuple) (cmp int) {
	var v []byte
	if i < len(td.fast) {
		start, stop := td.fast[i][0], td.fast[i][1]
		v = tup[start:stop]
	} else {
		v = tup.GetField(i)
	}
	return td.cmp.CompareValues(value, v, td.Types[i])
}

// Comparator returns the TupleDescriptor's TupleComparator.
func (td TupleDesc) Comparator() TupleComparator {
	return td.cmp
}

// Count returns the number of fields in the TupleDesc.
func (td TupleDesc) Count() int {
	return len(td.Types)
}

// IsNull returns true if the ith field of the Tuple is NULL.
func (td TupleDesc) IsNull(i int, tup Tuple) bool {
	b := td.GetField(i, tup)
	return b == nil
}

// GetBool reads a bool from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBool(i int, tup Tuple) (v bool, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readBool(b), true
	}
	return
}

// GetInt8 reads an int8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt8(i int, tup Tuple) (v int8, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt8(b), true
	}
	return
}

// GetUint8 reads a uint8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint8(i int, tup Tuple) (v uint8, ok bool) {
	td.expectEncoding(i, Uint8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint8(b), true
	}
	return
}

// GetInt16 reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt16(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, Int16Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt16(b), true
	}
	return
}

// GetUint16 reads a uint16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint16(i int, tup Tuple) (v uint16, ok bool) {
	td.expectEncoding(i, Uint16Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint16(b), true
	}
	return
}

// GetInt32 reads an int32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt32(i int, tup Tuple) (v int32, ok bool) {
	td.expectEncoding(i, Int32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt32(b), true
	}
	return
}

// GetUint32 reads a uint32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint32(i int, tup Tuple) (v uint32, ok bool) {
	td.expectEncoding(i, Uint32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint32(b), true
	}
	return
}

// GetInt64 reads an int64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt64(i int, tup Tuple) (v int64, ok bool) {
	td.expectEncoding(i, Int64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt64(b), true
	}
	return
}

// GetUint64 reads a uint64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint64(i int, tup Tuple) (v uint64, ok bool) {
	td.expectEncoding(i, Uint64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint64(b), true
	}
	return
}

// GetFloat32 reads a float32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat32(i int, tup Tuple) (v float32, ok bool) {
	td.expectEncoding(i, Float32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readFloat32(b), true
	}
	return
}

// GetFloat64 reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat64(i int, tup Tuple) (v float64, ok bool) {
	td.expectEncoding(i, Float64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readFloat64(b), true
	}
	return
}

// GetDecimal reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetDecimal(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, DecimalEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readString(b), true
	}
	return
}

// GetTimestamp reads a time.Time from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetTimestamp(i int, tup Tuple) (v time.Time, ok bool) {
	td.expectEncoding(i, TimestampEnc, DateEnc, DatetimeEnc, YearEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readTimestamp(b), true
	}
	return
}

// GetSqlTime reads a string encoded Time value from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetSqlTime(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, TimeEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readString(b), true
	}
	return
}

// GetYear reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetYear(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, YearEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt16(b), true
	}
	return
}

// GetString reads a string from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetString(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, StringEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readString(b)
		ok = true
	}
	return
}

// GetBytes reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBytes(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, ByteStringEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
		ok = true
	}
	return
}

// GetJSON reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetJSON(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, JSONEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
		ok = true
	}
	return
}

// GetGeometry reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetGeometry(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, GeometryEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
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
	for i := range td.Types {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true
		sb.WriteString(td.FormatValue(i, tup.GetField(i)))
	}
	sb.WriteString(" )")
	return sb.String()
}

func (td TupleDesc) FormatValue(i int, value []byte) string {
	if value == nil {
		return "NULL"
	}

	// todo(andy): complete cases
	switch td.Types[i].Enc {
	case Int8Enc:
		v := readInt8(value)
		return strconv.Itoa(int(v))
	case Uint8Enc:
		v := readUint8(value)
		return strconv.Itoa(int(v))
	case Int16Enc:
		v := readInt16(value)
		return strconv.Itoa(int(v))
	case Uint16Enc:
		v := readUint16(value)
		return strconv.Itoa(int(v))
	case Int32Enc:
		v := readInt32(value)
		return strconv.Itoa(int(v))
	case Uint32Enc:
		v := readUint32(value)
		return strconv.Itoa(int(v))
	case Int64Enc:
		v := readInt64(value)
		return strconv.FormatInt(v, 10)
	case Uint64Enc:
		v := readUint64(value)
		return strconv.FormatUint(v, 10)
	case Float32Enc:
		v := readFloat32(value)
		return fmt.Sprintf("%f", v)
	case Float64Enc:
		v := readFloat64(value)
		return fmt.Sprintf("%f", v)
	case StringEnc:
		return readString(value)
	case ByteStringEnc:
		return string(value)
	default:
		return string(value)
	}
}

// Equals returns true if |td| and |other| have equal type slices.
func (td TupleDesc) Equals(other TupleDesc) bool {
	if len(td.Types) != len(other.Types) {
		return false
	}
	for i, typ := range td.Types {
		if typ != other.Types[i] {
			return false
		}
	}
	return true
}
