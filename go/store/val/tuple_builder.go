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
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	builderBufferSize = 128
)

// OrdinalMapping is a mapping from one field ordering to another.
// It's used to construct index tuples from another index's tuples.
type OrdinalMapping []int

// NewIdentityOrdinalMapping returns a new OrdinalMapping that maps every ordinal to itself.
func NewIdentityOrdinalMapping(size int) OrdinalMapping {
	newMapping := make(OrdinalMapping, size)
	for i := 0; i < size; i++ {
		newMapping[i] = i
	}
	return newMapping
}

// MapOrdinal returns the ordinal of the field in the source tuple that maps to the |to| ordinal in the destination tuple.
func (om OrdinalMapping) MapOrdinal(to int) (from int) {
	from = om[to]
	return
}

// IsIdentityMapping returns true if this mapping is the identity mapping (i.e. every position is mapped
// to the same position and no columns are reordered).
func (om OrdinalMapping) IsIdentityMapping() bool {
	for i, mapping := range om {
		if i != mapping {
			return false
		}
	}
	return true
}

type TupleBuilder struct {
	Desc   TupleDesc
	fields [][]byte
	buf    []byte
	pos    ByteSize
}

func NewTupleBuilder(desc TupleDesc) *TupleBuilder {
	return &TupleBuilder{
		Desc:   desc,
		fields: make([][]byte, len(desc.Types)),
		buf:    make([]byte, builderBufferSize),
	}
}

// Build materializes a Tuple from the fields written to the TupleBuilder.
func (tb *TupleBuilder) Build(pool pool.BuffPool) (tup Tuple) {
	for i, typ := range tb.Desc.Types {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field: " + strconv.Itoa(i))
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

// BuildPrefix materializes a prefix Tuple from the first |k| fields written to the TupleBuilder.
func (tb *TupleBuilder) BuildPrefix(pool pool.BuffPool, k int) (tup Tuple) {
	for i, typ := range tb.Desc.Types[:k] {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field")
		}
	}
	values := tb.fields[:k]
	tup = NewTuple(pool, values...)
	tb.Recycle()
	return
}

// BuildPrefixNoRecycle materializes a prefix Tuple from the first |k| fields
// but does not call Recycle.
func (tb *TupleBuilder) BuildPrefixNoRecycle(pool pool.BuffPool, k int) (tup Tuple) {
	for i, typ := range tb.Desc.Types[:k] {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field")
		}
	}
	values := tb.fields[:k]
	tup = NewTuple(pool, values...)
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
	tb.ensureCapacity(int8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int8Size]
	writeBool(tb.fields[i], v)
	tb.pos += int8Size
}

// PutInt8 writes an int8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt8(i int, v int8) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.ensureCapacity(int8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int8Size]
	writeInt8(tb.fields[i], v)
	tb.pos += int8Size
}

// PutUint8 writes a uint8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint8(i int, v uint8) {
	tb.Desc.expectEncoding(i, Uint8Enc)
	tb.ensureCapacity(uint8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint8Size]
	writeUint8(tb.fields[i], v)
	tb.pos += uint8Size
}

// PutInt16 writes an int16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt16(i int, v int16) {
	tb.Desc.expectEncoding(i, Int16Enc)
	tb.ensureCapacity(int16Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int16Size]
	writeInt16(tb.fields[i], v)
	tb.pos += int16Size
}

// PutUint16 writes a uint16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint16(i int, v uint16) {
	tb.Desc.expectEncoding(i, Uint16Enc)
	tb.ensureCapacity(uint16Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint16Size]
	WriteUint16(tb.fields[i], v)
	tb.pos += uint16Size
}

// PutInt32 writes an int32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt32(i int, v int32) {
	tb.Desc.expectEncoding(i, Int32Enc)
	tb.ensureCapacity(int32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int32Size]
	writeInt32(tb.fields[i], v)
	tb.pos += int32Size
}

// PutUint32 writes a uint32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint32(i int, v uint32) {
	tb.Desc.expectEncoding(i, Uint32Enc)
	tb.ensureCapacity(uint32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint32Size]
	writeUint32(tb.fields[i], v)
	tb.pos += uint32Size
}

// PutInt64 writes an int64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt64(i int, v int64) {
	tb.Desc.expectEncoding(i, Int64Enc)
	tb.ensureCapacity(int64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64Size]
	writeInt64(tb.fields[i], v)
	tb.pos += int64Size
}

// PutUint64 writes a uint64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint64(i int, v uint64) {
	tb.Desc.expectEncoding(i, Uint64Enc)
	tb.ensureCapacity(uint64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+uint64Size]
	writeUint64(tb.fields[i], v)
	tb.pos += uint64Size
}

// PutFloat32 writes a float32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat32(i int, v float32) {
	tb.Desc.expectEncoding(i, Float32Enc)
	tb.ensureCapacity(float32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+float32Size]
	writeFloat32(tb.fields[i], v)
	tb.pos += float32Size
}

// PutFloat64 writes a float64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat64(i int, v float64) {
	tb.Desc.expectEncoding(i, Float64Enc)
	tb.ensureCapacity(float64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+float64Size]
	writeFloat64(tb.fields[i], v)
	tb.pos += float64Size
}

func (tb *TupleBuilder) PutBit(i int, v uint64) {
	tb.Desc.expectEncoding(i, Bit64Enc)
	tb.ensureCapacity(bit64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+bit64Size]
	writeBit64(tb.fields[i], v)
	tb.pos += bit64Size
}

func (tb *TupleBuilder) PutDecimal(i int, v decimal.Decimal) {
	tb.Desc.expectEncoding(i, DecimalEnc)
	sz := sizeOfDecimal(v)
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeDecimal(tb.fields[i], v)
	tb.pos += sz
}

// PutYear writes an int16-encoded year to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutYear(i int, v int16) {
	tb.Desc.expectEncoding(i, YearEnc)
	tb.ensureCapacity(yearSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+yearSize]
	writeYear(tb.fields[i], v)
	tb.pos += int16Size
}

func (tb *TupleBuilder) PutDate(i int, v time.Time) {
	tb.Desc.expectEncoding(i, DateEnc)
	tb.ensureCapacity(dateSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+dateSize]
	writeDate(tb.fields[i], v)
	tb.pos += dateSize
}

// PutSqlTime writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutSqlTime(i int, v int64) {
	tb.Desc.expectEncoding(i, TimeEnc)
	tb.ensureCapacity(timeSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+timeSize]
	writeTime(tb.fields[i], v)
	tb.pos += timeSize
}

func (tb *TupleBuilder) PutDatetime(i int, v time.Time) {
	tb.Desc.expectEncoding(i, DatetimeEnc)
	tb.ensureCapacity(datetimeSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+datetimeSize]
	writeDatetime(tb.fields[i], v)
	tb.pos += datetimeSize
}

func (tb *TupleBuilder) PutEnum(i int, v uint16) {
	tb.Desc.expectEncoding(i, EnumEnc)
	tb.ensureCapacity(enumSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+enumSize]
	writeEnum(tb.fields[i], v)
	tb.pos += enumSize
}

func (tb *TupleBuilder) PutSet(i int, v uint64) {
	tb.Desc.expectEncoding(i, SetEnc)
	tb.ensureCapacity(setSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+setSize]
	writeSet(tb.fields[i], v)
	tb.pos += setSize
}

// PutString writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutString(i int, v string) error {
	tb.Desc.expectEncoding(i, StringEnc)
	sz := ByteSize(len(v)) + 1
	offSz := 0
	if i > 0 {
		offSz = 2 * int(uint16Size)
	}
	if int(tb.pos)+len(v)+offSz > int(MaxTupleDataSize) {
		return analyzererrors.ErrInvalidRowLength.New(MaxTupleDataSize, int(tb.pos)+len(v)+int(offsetsSize(i)))
	}
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeString(tb.fields[i], v)
	tb.pos += sz
	return nil
}

// PutByteString writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutByteString(i int, v []byte) {
	tb.Desc.expectEncoding(i, ByteStringEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutJSON writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutJSON(i int, v []byte) {
	tb.Desc.expectEncoding(i, JSONEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutGeometry writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutGeometry(i int, v []byte) {
	tb.Desc.expectEncoding(i, GeometryEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeByteString(tb.fields[i], v)
	tb.pos += sz
}

// PutGeometryAddr writes a Geometry's address ref to the ith field
func (tb *TupleBuilder) PutGeometryAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, GeomAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

// PutHash128 writes a hash128 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutHash128(i int, v []byte) {
	tb.Desc.expectEncoding(i, Hash128Enc)
	tb.ensureCapacity(hash128Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+hash128Size]
	writeHash128(tb.fields[i], v)
	tb.pos += hash128Size
}

// PutExtended writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutExtended(i int, v []byte) {
	tb.Desc.expectEncoding(i, ExtendedEnc)
	sz := ByteSize(len(v))
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeExtended(tb.Desc.Handlers[i], tb.fields[i], v)
	tb.pos += sz
}

// PutExtendedAddr writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutExtendedAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, ExtendedAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

// PutRaw writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutRaw(i int, buf []byte) {
	if buf == nil {
		// todo(andy): does it make sense to
		//  allow/expect nulls here?
		return
	}
	sz := ByteSize(len(buf))
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+sz]
	writeRaw(tb.fields[i], buf)
	tb.pos += sz
}

// PutCommitAddr writes a commit's address ref to the ith field
// of the Tuple being built.
func (tb *TupleBuilder) PutCommitAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, CommitAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

// PutBytesAddr writes a blob's address ref to the ith field
// of the Tuple being built.
func (tb *TupleBuilder) PutBytesAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, BytesAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

// PutStringAddr writes a string's address ref to the ith field
// of the Tuple being built.
func (tb *TupleBuilder) PutStringAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, StringAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

// PutJSONAddr writes a JSON string's address ref to the ith field
// of the Tuple being built.
func (tb *TupleBuilder) PutJSONAddr(i int, v hash.Hash) {
	tb.Desc.expectEncoding(i, JSONAddrEnc)
	tb.ensureCapacity(hash.ByteLen)
	tb.putAddr(i, v)
}

func (tb *TupleBuilder) putAddr(i int, v hash.Hash) {
	tb.fields[i] = tb.buf[tb.pos : tb.pos+hash.ByteLen]
	writeAddr(tb.fields[i], v[:])
	tb.pos += hash.ByteLen
}

func (tb *TupleBuilder) ensureCapacity(sz ByteSize) {
	need := int(tb.pos+sz) - len(tb.buf)
	if need > 0 {
		for i := 0; i < need; i++ {
			tb.buf = append(tb.buf, byte(0))
		}
	}
}

// PutCell writes a Cell to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutCell(i int, v Cell) {
	tb.Desc.expectEncoding(i, CellEnc)
	tb.ensureCapacity(cellSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+cellSize]
	writeCell(tb.fields[i], v)
	tb.pos += cellSize
}
