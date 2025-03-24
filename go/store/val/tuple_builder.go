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
	"context"
	"fmt"
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

var defaultTupleLengthTarget int64 = (1 << 11)

type TupleBuilder struct {
	Desc              TupleDesc
	fields            [][]byte
	buf               []byte
	pos               int64
	tupleLengthTarget int64 // The max tuple length before the tuple builder attempts to outline values.
	outlineSize       int64 // The size of the tuple if every oversized type is outlined
	inlineSize        int64 // The size of the tuple if every oversized type is inlined
	vs                ValueStore
}

func NewTupleBuilder(desc TupleDesc, vs ValueStore) *TupleBuilder {
	return &TupleBuilder{
		Desc:              desc,
		fields:            make([][]byte, len(desc.Types)),
		buf:               make([]byte, builderBufferSize),
		vs:                vs,
		tupleLengthTarget: defaultTupleLengthTarget,
	}
}

// Build materializes a Tuple from the fields written to the TupleBuilder.
func (tb *TupleBuilder) Build(pool pool.BuffPool) (tup Tuple, err error) {
	for i, typ := range tb.Desc.Types {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field: " + strconv.Itoa(i))
		}
	}
	return tb.BuildPermissive(pool, tb.vs)
}

// BuildPermissive materializes a Tuple from the fields
// written to the TupleBuilder without validating nullability.
func (tb *TupleBuilder) BuildPermissive(pool pool.BuffPool, vs ValueStore) (tup Tuple, err error) {
	// TODO: add context parameter
	ctx := context.Background()
	// Values may get passed into the tuple builder in either in-band or out-of-band form.
	// In the best case, we don't need to convert any of them, so the TupleBuilder initially stores them in the form they're given.
	// But we track the tuple size if they're all inlined vs the tuple size if they're all out-of-band,
	// Then use this to determine which values need to be stored out of band.
	totalSize := tb.inlineSize
	if totalSize > tb.tupleLengthTarget {
		// We're above the size limit, begin converting to out-of-band storage.
		for i, descType := range tb.Desc.Types {
			if IsAdaptiveEncoding(descType.Enc) {
				adaptiveValue := AdaptiveValue(tb.fields[i])
				outlineSize := adaptiveValue.outOfBandSize()
				inlineSize := adaptiveValue.inlineSize()

				// We only outline a field if the outlined size is shorter than the inlined size.
				if outlineSize < inlineSize {
					if !adaptiveValue.IsOutOfBand() {
						outline, err := adaptiveValue.convertToOutOfBand(ctx, tb.vs, nil)
						if err != nil {
							return nil, err
						}
						tb.PutRaw(i, outline)
					}

					totalSize += outlineSize - inlineSize
				}
			}

			if totalSize <= tb.tupleLengthTarget {
				// We have enough space, mark all the remaining columns as inline
				for j, descType := range tb.Desc.Types[i+1:] {
					if IsAdaptiveEncoding(descType.Enc) {
						adaptiveValue := AdaptiveValue(tb.fields[j+i+1])
						if !adaptiveValue.isInlined() {
							inline, err := adaptiveValue.convertToInline(ctx, tb.vs, nil)
							if err != nil {
								return nil, err
							}
							tb.PutRaw(j+i+1, inline)
						}
					}
				}
				break
			}
		}
	}
	if totalSize > tb.tupleLengthTarget {
		return Tuple{}, fmt.Errorf("nable to create tuple under the target legnth. This should not be possible")
	}
	values := tb.fields[:tb.Desc.Count()]
	tup = NewTuple(pool, values...)
	tb.Recycle()
	return tup, nil
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

func (tb *TupleBuilder) addSize(sz ByteSize) {
	tb.inlineSize += int64(sz)
	tb.outlineSize += int64(sz)
}

// PutBool writes a bool to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutBool(i int, v bool) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.ensureCapacity(int8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(int8Size)]
	writeBool(tb.fields[i], v)
	tb.pos += int64(int8Size)
	tb.addSize(int8Size)
}

// PutInt8 writes an int8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt8(i int, v int8) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.ensureCapacity(int8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(int8Size)]
	writeInt8(tb.fields[i], v)
	tb.pos += int64(int8Size)
	tb.addSize(int8Size)
}

// PutUint8 writes a uint8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint8(i int, v uint8) {
	tb.Desc.expectEncoding(i, Uint8Enc)
	tb.ensureCapacity(uint8Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(1)]
	writeUint8(tb.fields[i], v)
	tb.pos += int64(uint8Size)
	tb.addSize(int8Size)
}

// PutInt16 writes an int16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt16(i int, v int16) {
	tb.Desc.expectEncoding(i, Int16Enc)
	tb.ensureCapacity(int16Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(int16Size)]
	writeInt16(tb.fields[i], v)
	tb.pos += int64(int16Size)
	tb.addSize(int16Size)
}

// PutUint16 writes a uint16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint16(i int, v uint16) {
	tb.Desc.expectEncoding(i, Uint16Enc)
	tb.ensureCapacity(uint16Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(uint16Size)]
	WriteUint16(tb.fields[i], v)
	tb.pos += int64(uint16Size)
	tb.addSize(int16Size)
}

// PutInt32 writes an int32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt32(i int, v int32) {
	tb.Desc.expectEncoding(i, Int32Enc)
	tb.ensureCapacity(int32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(int32Size)]
	writeInt32(tb.fields[i], v)
	tb.pos += int64(int32Size)
	tb.addSize(int32Size)
}

// PutUint32 writes a uint32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint32(i int, v uint32) {
	tb.Desc.expectEncoding(i, Uint32Enc)
	tb.ensureCapacity(uint32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(uint32Size)]
	writeUint32(tb.fields[i], v)
	tb.pos += int64(uint32Size)
	tb.addSize(int32Size)
}

// PutInt64 writes an int64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt64(i int, v int64) {
	tb.Desc.expectEncoding(i, Int64Enc)
	tb.ensureCapacity(int64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(int64Size)]
	writeInt64(tb.fields[i], v)
	tb.pos += int64(int64Size)
	tb.addSize(int64Size)
}

// PutUint64 writes a uint64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint64(i int, v uint64) {
	tb.Desc.expectEncoding(i, Uint64Enc)
	tb.ensureCapacity(uint64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(uint64Size)]
	writeUint64(tb.fields[i], v)
	tb.pos += int64(uint64Size)
	tb.addSize(int64Size)
}

// PutFloat32 writes a float32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat32(i int, v float32) {
	tb.Desc.expectEncoding(i, Float32Enc)
	tb.ensureCapacity(float32Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(float32Size)]
	writeFloat32(tb.fields[i], v)
	tb.pos += int64(float32Size)
	tb.addSize(float32Size)
}

// PutFloat64 writes a float64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat64(i int, v float64) {
	tb.Desc.expectEncoding(i, Float64Enc)
	tb.ensureCapacity(float64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(float64Size)]
	writeFloat64(tb.fields[i], v)
	tb.pos += int64(float64Size)
	tb.addSize(float64Size)
}

func (tb *TupleBuilder) PutBit(i int, v uint64) {
	tb.Desc.expectEncoding(i, Bit64Enc)
	tb.ensureCapacity(bit64Size)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(bit64Size)]
	writeBit64(tb.fields[i], v)
	tb.pos += int64(bit64Size)
	tb.addSize(bit64Size)
}

func (tb *TupleBuilder) PutDecimal(i int, v decimal.Decimal) {
	tb.Desc.expectEncoding(i, DecimalEnc)
	sz := sizeOfDecimal(v)
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeDecimal(tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
}

// PutYear writes an int16-encoded year to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutYear(i int, v int16) {
	tb.Desc.expectEncoding(i, YearEnc)
	tb.ensureCapacity(yearSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(yearSize)]
	writeYear(tb.fields[i], v)
	tb.pos += int64(int16Size)
	tb.addSize(int16Size)
}

func (tb *TupleBuilder) PutDate(i int, v time.Time) {
	tb.Desc.expectEncoding(i, DateEnc)
	tb.ensureCapacity(dateSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(dateSize)]
	writeDate(tb.fields[i], v)
	tb.pos += int64(dateSize)
	tb.addSize(dateSize)
}

// PutSqlTime writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutSqlTime(i int, v int64) {
	tb.Desc.expectEncoding(i, TimeEnc)
	tb.ensureCapacity(timeSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(timeSize)]
	writeTime(tb.fields[i], v)
	tb.pos += int64(timeSize)
	tb.addSize(timeSize)
}

func (tb *TupleBuilder) PutDatetime(i int, v time.Time) {
	tb.Desc.expectEncoding(i, DatetimeEnc)
	tb.ensureCapacity(datetimeSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(datetimeSize)]
	writeDatetime(tb.fields[i], v)
	tb.pos += int64(datetimeSize)
	tb.addSize(datetimeSize)
}

func (tb *TupleBuilder) PutEnum(i int, v uint16) {
	tb.Desc.expectEncoding(i, EnumEnc)
	tb.ensureCapacity(enumSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(enumSize)]
	writeEnum(tb.fields[i], v)
	tb.pos += int64(enumSize)
	tb.addSize(enumSize)

}

func (tb *TupleBuilder) PutSet(i int, v uint64) {
	tb.Desc.expectEncoding(i, SetEnc)
	tb.ensureCapacity(setSize)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(setSize)]
	writeSet(tb.fields[i], v)
	tb.pos += int64(setSize)
	tb.addSize(setSize)

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
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeString(tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
	return nil
}

// PutByteString writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutByteString(i int, v []byte) {
	tb.Desc.expectEncoding(i, ByteStringEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeByteString(tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
}

// PutJSON writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutJSON(i int, v []byte) {
	tb.Desc.expectEncoding(i, JSONEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeByteString(tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
}

// PutGeometry writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutGeometry(i int, v []byte) {
	tb.Desc.expectEncoding(i, GeometryEnc)
	sz := ByteSize(len(v)) + 1
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeByteString(tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
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
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(hash128Size)]
	writeHash128(tb.fields[i], v)
	tb.pos += int64(hash128Size)
	tb.addSize(hash128Size)
}

// PutExtended writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutExtended(i int, v []byte) {
	tb.Desc.expectEncoding(i, ExtendedEnc)
	sz := ByteSize(len(v))
	tb.ensureCapacity(sz)
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeExtended(tb.Desc.Handlers[i], tb.fields[i], v)
	tb.pos += int64(sz)
	tb.addSize(sz)
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
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(sz)]
	writeRaw(tb.fields[i], buf)
	tb.pos += int64(sz)
	tb.addSize(sz)
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
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(hash.ByteLen)]
	writeAddr(tb.fields[i], v[:])
	tb.pos += int64(hash.ByteLen)
	tb.addSize(hash.ByteLen)
}

func (tb *TupleBuilder) ensureCapacity(sz ByteSize) {
	need := int(tb.pos+int64(sz)) - len(tb.buf)
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
	tb.fields[i] = tb.buf[tb.pos : tb.pos+int64(cellSize)]
	writeCell(tb.fields[i], v)
	tb.pos += int64(cellSize)
	tb.addSize(cellSize)
}

func (tb *TupleBuilder) PutAdaptiveBytesFromInline(ctx context.Context, i int, v []byte) error {
	tb.Desc.expectEncoding(i, BytesAdaptiveEnc)
	if int64(len(v)) > tb.tupleLengthTarget {
		// Inline value is too large. We must outline it.
		tb.ensureCapacity(29)
		blobLength := uint64(len(v))
		lengthSize, _ := makeVarInt(blobLength, tb.buf[tb.pos:])

		blobHash, err := tb.vs.WriteBytes(ctx, []byte(v))
		if err != nil {
			return err
		}
		copy(tb.buf[tb.pos+int64(lengthSize):], blobHash[:])
		field := tb.buf[tb.pos : tb.pos+int64(lengthSize)+hash.ByteLen]
		tb.fields[i] = field
		tb.pos += int64(lengthSize) + hash.ByteLen
		return nil
	}
	sz := ByteSize(len(v)) + 1 // include extra header byte
	tb.ensureCapacity(sz)
	field := AdaptiveValue(tb.buf[tb.pos : tb.pos+int64(sz)])
	tb.fields[i] = field
	field[0] = 0 // Mark this as inline
	copy(field[1:], v)
	tb.pos += int64(sz)
	tb.inlineSize += int64(sz)
	tb.outlineSize += field.outOfBandSize()
	return nil
}

func (tb *TupleBuilder) PutAdaptiveStringFromInline(ctx context.Context, i int, v string) error {
	tb.Desc.expectEncoding(i, StringAdaptiveEnc)
	if int64(len(v)) > tb.tupleLengthTarget {
		// Inline value is too large. We must outline it.
		maxLengthBytes := 9
		tb.ensureCapacity(ByteSize(hash.ByteLen + maxLengthBytes))
		blobLength := uint64(len(v))
		lengthSize, _ := makeVarInt(blobLength, tb.buf[tb.pos:])

		blobHash, err := tb.vs.WriteBytes(ctx, []byte(v))
		if err != nil {
			return err
		}
		copy(tb.buf[tb.pos+int64(lengthSize):], blobHash[:])
		field := tb.buf[tb.pos : tb.pos+int64(lengthSize)+hash.ByteLen]
		tb.fields[i] = field
		tb.pos += int64(lengthSize) + hash.ByteLen
		return nil
	}
	sz := ByteSize(len(v)) + 1 // include extra header byte
	tb.ensureCapacity(sz)
	field := AdaptiveValue(tb.buf[tb.pos : tb.pos+int64(sz)])
	tb.fields[i] = field
	field[0] = 0 // Mark this as inline
	copy(field[1:], v)
	tb.pos += int64(sz)
	tb.inlineSize += int64(sz)
	tb.outlineSize += field.outOfBandSize()
	return nil
}

func (tb *TupleBuilder) PutAdaptiveBytesFromOutline(i int, v *ByteArray) {
	tb.Desc.expectEncoding(i, BytesAdaptiveEnc)

	maxLengthBytes := 9
	tb.ensureCapacity(ByteSize(hash.ByteLen + maxLengthBytes))
	blobLength := uint64(v.MaxByteLength())
	lengthSize, _ := makeVarInt(blobLength, tb.buf[tb.pos:])

	copy(tb.buf[tb.pos+int64(lengthSize):], v.Addr[:])
	field := tb.buf[tb.pos : tb.pos+int64(lengthSize)+hash.ByteLen]
	tb.fields[i] = field
	tb.pos += int64(lengthSize) + hash.ByteLen
}

func (tb *TupleBuilder) PutAdaptiveStringFromOutline(i int, v *TextStorage) {
	tb.Desc.expectEncoding(i, StringAdaptiveEnc)

	maxLengthBytes := 9
	tb.ensureCapacity(ByteSize(hash.ByteLen + maxLengthBytes))
	blobLength := uint64(v.MaxByteLength())
	lengthSize, _ := makeVarInt(blobLength, tb.buf[tb.pos:])

	copy(tb.buf[tb.pos+int64(lengthSize):], v.Addr[:])
	field := tb.buf[tb.pos : tb.pos+int64(lengthSize)+hash.ByteLen]
	tb.fields[i] = field
	tb.pos += int64(lengthSize) + hash.ByteLen
}
