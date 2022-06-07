// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"
	"math"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

const initialBufferSize = 2048

func EncodeValue(v Value, nbf *NomsBinFormat) (chunks.Chunk, error) {
	if v.Kind() == UnknownKind {
		return chunks.EmptyChunk, ErrUnknownType
	}

	w := binaryNomsWriter{make([]byte, 4), 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return chunks.EmptyChunk, err
	}

	return chunks.NewChunk(w.data()), nil
}

func decodeFromBytes(data []byte, vrw ValueReadWriter) (Value, error) {
	dec := newValueDecoder(data, vrw)
	v, err := dec.readValue(vrw.Format())

	if err != nil {
		return nil, err
	}

	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v, nil
}

func decodeFromBytesWithValidation(data []byte, vrw ValueReadWriter) (Value, error) {
	r := binaryNomsReader{data, 0}
	dec := newValueDecoderWithValidation(r, vrw)
	v, err := dec.readValue(vrw.Format())

	if err != nil {
		return nil, err
	}

	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v, nil
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vrw ValueReadWriter) (Value, error) {
	d.PanicIfTrue(c.IsEmpty())
	return decodeFromBytes(c.Data(), vrw)
}

type nomsWriter interface {
	writeBool(b bool)
	writeCount(count uint64)
	writeHash(h hash.Hash)
	writeFloat(v Float, nbf *NomsBinFormat)
	writeInt(v Int)
	writeUint(v Uint)
	writeString(v string)
	writeUint8(v uint8)
	writeUint16(v uint16)
	writeRaw(buff []byte)
}

type binaryNomsReader struct {
	buff   []byte
	offset uint32
}

func (b *binaryNomsReader) readBytes(count uint32) []byte {
	v := b.buff[b.offset : b.offset+count]
	b.offset += count
	return v
}

func (b *binaryNomsReader) readCopyOfBytes(count uint32) []byte {
	v := make([]byte, count)
	copy(v, b.buff[b.offset:b.offset+count])
	b.offset += count
	return v
}

func (b *binaryNomsReader) skipBytes(count uint32) {
	b.offset += count
}

func (b *binaryNomsReader) pos() uint32 {
	return b.offset
}

func (b *binaryNomsReader) readUint8() uint8 {
	v := uint8(b.buff[b.offset])
	b.offset++
	return v
}

func (b *binaryNomsReader) peekUint8() uint8 {
	return uint8(b.buff[b.offset])
}

func (b *binaryNomsReader) skipUint8() {
	b.offset++
}

func (b *binaryNomsReader) readUint16() uint16 {
	v := binary.BigEndian.Uint16(b.buff[b.offset:])
	b.offset += 2
	return v
}

func (b *binaryNomsReader) PeekKind() NomsKind {
	return NomsKind(b.peekUint8())
}

func (b *binaryNomsReader) ReadKind() NomsKind {
	return NomsKind(b.readUint8())
}

func (b *binaryNomsReader) skipKind() {
	b.skipUint8()
}

func (b *binaryNomsReader) readCount() uint64 {
	return b.ReadUint()
}

func (b *binaryNomsReader) skipCount() {
	b.skipUint()
}

func (b *binaryNomsReader) ReadFloat(nbf *NomsBinFormat) float64 {
	if isFormat_7_18(nbf) {
		i := b.ReadInt()
		exp := b.ReadInt()
		return fracExpToFloat(i, int(exp))
	} else {
		floatbits := binary.BigEndian.Uint64(b.readBytes(8))
		return math.Float64frombits(floatbits)
	}
}

func (b *binaryNomsReader) ReadDecimal() (decimal.Decimal, error) {
	size := uint32(b.readUint16())
	db := b.readBytes(size)

	var dec decimal.Decimal
	err := dec.GobDecode(db)
	return dec, err
}

func (b *binaryNomsReader) ReadTimestamp() (time.Time, error) {
	data := b.readBytes(timestampNumBytes)

	var t time.Time
	err := t.UnmarshalBinary(data)
	return t, err
}

func (b *binaryNomsReader) skipFloat(nbf *NomsBinFormat) {
	if isFormat_7_18(nbf) {
		b.skipInt()
		b.skipInt()
	} else {
		b.skipBytes(8)
	}
}

func (b *binaryNomsReader) skipInt() {
	maxOffset := b.offset + 10
	for ; b.offset < maxOffset; b.offset++ {
		if b.buff[b.offset]&0x80 == 0 {
			b.offset++
			return
		}
	}
}

func (b *binaryNomsReader) ReadInt() int64 {
	v, count := unrolledDecodeVarint(b.buff[b.offset:])
	b.offset += uint32(count)
	return v
}

func (b *binaryNomsReader) ReadUint() uint64 {
	v, count := unrolledDecodeUVarint(b.buff[b.offset:])
	b.offset += uint32(count)
	return v
}

func unrolledDecodeUVarint(buf []byte) (uint64, int) {
	b := uint64(buf[0])
	if b < 0x80 {
		return b, 1
	}

	x := b & 0x7f
	b = uint64(buf[1])
	if b < 0x80 {
		return x | (b << 7), 2
	}

	x |= (b & 0x7f) << 7
	b = uint64(buf[2])
	if b < 0x80 {
		return x | (b << 14), 3
	}

	x |= (b & 0x7f) << 14
	b = uint64(buf[3])
	if b < 0x80 {
		return x | (b << 21), 4
	}

	x |= (b & 0x7f) << 21
	b = uint64(buf[4])
	if b < 0x80 {
		return x | (b << 28), 5
	}

	x |= (b & 0x7f) << 28
	b = uint64(buf[5])
	if b < 0x80 {
		return x | (b << 35), 6
	}

	x |= (b & 0x7f) << 35
	b = uint64(buf[6])
	if b < 0x80 {
		return x | (b << 42), 7
	}

	x |= (b & 0x7f) << 42
	b = uint64(buf[7])
	if b < 0x80 {
		return x | (b << 49), 8
	}

	x |= (b & 0x7f) << 49
	b = uint64(buf[8])
	if b < 0x80 {
		return x | (b << 56), 9
	}

	x |= (b & 0x7f) << 56
	b = uint64(buf[9])
	if b == 1 {
		return x | (1 << 63), 10
	}

	return 0, -10
}

func unrolledDecodeVarint(buf []byte) (int64, int) {
	ux, n := unrolledDecodeUVarint(buf) // ok to continue in presence of error
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n
}

func (b *binaryNomsReader) skipUint() {
	maxOffset := b.offset + 10
	for ; b.offset < maxOffset; b.offset++ {
		if b.buff[b.offset]&0x80 == 0 {
			b.offset++
			return
		}
	}
}

func (b *binaryNomsReader) ReadBool() bool {
	return b.readUint8() == 1
}

func (b *binaryNomsReader) skipBool() {
	b.skipUint8()
}

func (b *binaryNomsReader) ReadString() string {
	size := uint32(b.readCount())
	strBytes := b.buff[b.offset : b.offset+size]
	b.offset += size
	return *(*string)(unsafe.Pointer(&strBytes))
}

func (b *binaryNomsReader) ReadInlineBlob() []byte {
	size := uint32(b.readUint16())
	bytes := b.buff[b.offset : b.offset+size]
	b.offset += size
	return bytes
}

func (b *binaryNomsReader) readTupleRowStorage() []byte {
	size := uint32(b.readUint16())
	// start at offset-3, to include the kind byte + Uint16 for size...
	bytes := b.buff[b.offset-3 : b.offset+size]
	b.offset += size
	return bytes
}

func (b *binaryNomsReader) ReadUUID() uuid.UUID {
	id := uuid.UUID{}
	copy(id[:uuidNumBytes], b.readBytes(uuidNumBytes))
	return id
}

func (b *binaryNomsReader) skipString() {
	size := uint32(b.readCount())
	b.offset += size
}

func (b *binaryNomsReader) readHash() hash.Hash {
	h := hash.Hash{}
	copy(h[:], b.buff[b.offset:b.offset+hash.ByteLen])
	b.offset += hash.ByteLen
	return h
}

func (b *binaryNomsReader) skipHash() {
	b.offset += hash.ByteLen
}

func (b *binaryNomsReader) byteSlice(start, end uint32) []byte {
	return b.buff[start:end]
}

type binaryNomsWriter struct {
	buff   []byte
	offset uint32
}

func newBinaryNomsWriterWithSizeHint(sizeHint uint64) binaryNomsWriter {
	size := uint32(initialBufferSize)
	if sizeHint >= math.MaxUint32 {
		size = math.MaxUint32
	} else if sizeHint > uint64(size) {
		size = uint32(sizeHint)
	}

	return binaryNomsWriter{make([]byte, size), 0}
}

func newBinaryNomsWriter() binaryNomsWriter {
	return binaryNomsWriter{make([]byte, initialBufferSize), 0}
}

func (b *binaryNomsWriter) data() []byte {
	return b.buff[0:b.offset]
}

func (b *binaryNomsWriter) reset() {
	b.offset = 0
}

const (
	GigsHalf = 1 << 29
	Gigs2    = 1 << 31
)

func (b *binaryNomsWriter) ensureCapacity(n uint32) {
	length := uint64(len(b.buff))
	minLength := uint64(b.offset) + uint64(n)
	if length >= minLength {
		return
	}

	old := b.buff

	if minLength > math.MaxUint32 {
		panic("overflow")
	}

	for minLength > length {
		length = length * 2

		if length >= Gigs2 {
			length = Gigs2
			break
		}
	}

	for minLength > length {
		length += GigsHalf

		if length >= math.MaxUint32 {
			length = math.MaxUint32
			break
		}
	}

	b.buff = make([]byte, length)
	copy(b.buff, old)
}

func (b *binaryNomsWriter) writeUint8(v uint8) {
	b.ensureCapacity(1)
	b.buff[b.offset] = byte(v)
	b.offset++
}

func (b *binaryNomsWriter) writeCount(v uint64) {
	b.ensureCapacity(binary.MaxVarintLen64)
	count := binary.PutUvarint(b.buff[b.offset:], v)
	b.offset += uint32(count)
}

func (b *binaryNomsWriter) writeInt(v Int) {
	b.ensureCapacity(binary.MaxVarintLen64)
	count := binary.PutVarint(b.buff[b.offset:], int64(v))
	b.offset += uint32(count)
}

func (b *binaryNomsWriter) writeUint(v Uint) {
	b.ensureCapacity(binary.MaxVarintLen64)
	count := binary.PutUvarint(b.buff[b.offset:], uint64(v))
	b.offset += uint32(count)
}

func (b *binaryNomsWriter) writeUint16(v uint16) {
	b.ensureCapacity(2)
	binary.BigEndian.PutUint16(b.buff[b.offset:], v)
	b.offset += 2
}

func (b *binaryNomsWriter) writeFloat(v Float, nbf *NomsBinFormat) {
	if isFormat_7_18(nbf) {
		b.ensureCapacity(binary.MaxVarintLen64 * 2)
		i, exp := float64ToIntExp(float64(v))
		count := binary.PutVarint(b.buff[b.offset:], i)
		b.offset += uint32(count)
		count = binary.PutVarint(b.buff[b.offset:], int64(exp))
		b.offset += uint32(count)
	} else {
		b.ensureCapacity(8)
		binary.BigEndian.PutUint64(b.buff[b.offset:], math.Float64bits(float64(v)))
		b.offset += 8
	}
}

func (b *binaryNomsWriter) writeBool(v bool) {
	if v {
		b.writeUint8(uint8(1))
	} else {
		b.writeUint8(uint8(0))
	}
}

func (b *binaryNomsWriter) writeString(v string) {
	size := uint32(len(v))
	b.writeCount(uint64(size))

	b.ensureCapacity(size)
	copy(b.buff[b.offset:], v)
	b.offset += size
}

func (b *binaryNomsWriter) writeHash(h hash.Hash) {
	b.ensureCapacity(hash.ByteLen)
	copy(b.buff[b.offset:], h[:])
	b.offset += hash.ByteLen
}

func (b *binaryNomsWriter) writeRaw(buff []byte) {
	size := uint32(len(buff))
	b.ensureCapacity(size)
	copy(b.buff[b.offset:], buff)
	b.offset += size
}
