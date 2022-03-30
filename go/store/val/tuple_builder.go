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
	"time"

	"github.com/dolthub/dolt/go/store/pool"
)

// OrdinalMapping is a mapping from one field ordering to another.
// It's used to construct index tuples from another index's tuples.
type OrdinalMapping []int

func (om OrdinalMapping) MapOrdinal(to int) (from int) {
	from = om[to]
	return
}

type TupleBuilder struct {
	Desc TupleDesc

	buf [MaxTupleDataSize]byte
	pos ByteSize

	fields [MaxTupleFields][]byte
}

func NewTupleBuilder(desc TupleDesc) *TupleBuilder {
	return &TupleBuilder{Desc: desc}
}

// Build materializes a Tuple from the fields written to the TupleBuilder.
func (tb *TupleBuilder) Build(pool pool.BuffPool) (tup Tuple) {
	for i, typ := range tb.Desc.Types {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field")
		}
	}
	return tb.BuildPermissive(pool)
}

// BuildPermissive materializes a Tuple from the fields
// written to the TupleBuilder without validating nullability.
func (tb *TupleBuilder) BuildPermissive(pool pool.BuffPool) (tup Tuple) {
	values := tb.fields[:tb.Desc.Count()]
	tup = NewTuple(pool, values...)
	tb.Recycle()
	return
}

// Recycle resets the TupleBuilder so it can build a new Tuple.
func (tb *TupleBuilder) Recycle() {
	for i := 0; i < tb.Desc.Count(); i++ {
		tb.fields[i] = nil
	}
	tb.pos = 0
}

// PutBool writes a bool to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutBool(i int, v bool) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int8Size]
	writeBool(tb.fields[i], v)
	tb.pos += int8Size
}

// PutInt8 writes an int8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt8(i int, v int8) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int8Size]
	writeInt8(tb.fields[i], v)
	tb.pos += int8Size
}

// PutUint8 writes a uint8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint8(i int, v uint8) {
	tb.Desc.expectEncoding(i, Uint8Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint8Size]
	writeUint8(tb.fields[i], v)
	tb.pos += uint8Size
}

// PutInt16 writes an int16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt16(i int, v int16) {
	tb.Desc.expectEncoding(i, Int16Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int16Size]
	writeInt16(tb.fields[i], v)
	tb.pos += int16Size
}

// PutUint16 writes a uint16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint16(i int, v uint16) {
	tb.Desc.expectEncoding(i, Uint16Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint16Size]
	writeUint16(tb.fields[i], v)
	tb.pos += uint16Size
}

// PutInt32 writes an int32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt32(i int, v int32) {
	tb.Desc.expectEncoding(i, Int32Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int32Size]
	writeInt32(tb.fields[i], v)
	tb.pos += int32Size
}

// PutUint32 writes a uint32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint32(i int, v uint32) {
	tb.Desc.expectEncoding(i, Uint32Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint32Size]
	writeUint32(tb.fields[i], v)
	tb.pos += uint32Size
}

// PutInt64 writes an int64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt64(i int, v int64) {
	tb.Desc.expectEncoding(i, Int64Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64Size]
	writeInt64(tb.fields[i], v)
	tb.pos += int64Size
}

// PutUint64 writes a uint64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint64(i int, v uint64) {
	tb.Desc.expectEncoding(i, Uint64Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint64Size]
	writeUint64(tb.fields[i], v)
	tb.pos += uint64Size
}

// PutFloat32 writes a float32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat32(i int, v float32) {
	tb.Desc.expectEncoding(i, Float32Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+float32Size]
	writeFloat32(tb.fields[i], v)
	tb.pos += float32Size
}

// PutFloat64 writes a float64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat64(i int, v float64) {
	tb.Desc.expectEncoding(i, Float64Enc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+float64Size]
	writeFloat64(tb.fields[i], v)
	tb.pos += float64Size
}

func (tb *TupleBuilder) PutTimestamp(i int, v time.Time) {
	tb.Desc.expectEncoding(i, DateEnc, DatetimeEnc, TimestampEnc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+timestampSize]
	writeTimestamp(tb.fields[i], v)
	tb.pos += timestampSize
}

// PutSqlTime writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutSqlTime(i int, v string) {
	tb.Desc.expectEncoding(i, TimeEnc)
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeString(tb.fields[i], v)
	tb.pos += sz
}

// PutYear writes an int16-encoded year to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutYear(i int, v int16) {
	// todo(andy): yearSize, etc?
	tb.Desc.expectEncoding(i, YearEnc)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int16Size]
	writeInt16(tb.fields[i], v)
	tb.pos += int16Size
}

func (tb *TupleBuilder) PutDecimal(i int, v string) {
	tb.Desc.expectEncoding(i, DecimalEnc)
	// todo(andy): temporary implementation
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeString(tb.fields[i], v)
	tb.pos += sz
}

// PutString writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutString(i int, v string) {
	tb.Desc.expectEncoding(i, StringEnc)
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeString(tb.fields[i], v)
	tb.pos += sz
}

// PutByteString writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutByteString(i int, v []byte) {
	tb.Desc.expectEncoding(i, ByteStringEnc)
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutJSON writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutJSON(i int, v []byte) {
	tb.Desc.expectEncoding(i, JSONEnc)
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutGeometry writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutGeometry(i int, v []byte) {
	tb.Desc.expectEncoding(i, GeometryEnc)
	sz := ByteSize(len(v)) + 1
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutRaw writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutRaw(i int, buf []byte) {
	if buf == nil {
		// todo(andy): does it make senes to
		//  allow/expect nulls here?
		return
	}
	sz := ByteSize(len(buf))
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeRaw(tb.fields[i], buf)
	tb.pos += sz
}
