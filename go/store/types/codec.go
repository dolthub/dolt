// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

const initialBufferSize = 2048

type valueBytes interface {
	valueBytes(*NomsBinFormat) []byte
}

func EncodeValue(v Value, nbf *NomsBinFormat) chunks.Chunk {
	switch v := v.(type) {
	case valueBytes:
		return chunks.NewChunk(v.valueBytes(nbf))
	case *Type:
		w := newBinaryNomsWriter()
		v.writeTo(&w, nbf)
		return chunks.NewChunk(w.data())
	}

	panic("unreachable")
}

func decodeFromBytes(data []byte, vrw ValueReadWriter) Value {
	dec := newValueDecoder(data, vrw)
	v := dec.readValue(vrw.Format())
	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v
}

func decodeFromBytesWithValidation(data []byte, vrw ValueReadWriter) Value {
	r := binaryNomsReader{data, 0}
	dec := newValueDecoderWithValidation(r, vrw)
	v := dec.readValue(vrw.Format())
	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vrw ValueReadWriter) Value {
	d.PanicIfTrue(c.IsEmpty())
	return decodeFromBytes(c.Data(), vrw)
}

type nomsWriter interface {
	writeBool(b bool)
	writeBytes(v []byte)
	writeCount(count uint64)
	writeHash(h hash.Hash)
	writeFloat(v Float, nbf *NomsBinFormat)
	writeInt(v Int)
	writeUint(v Uint)
	writeString(v string)
	writeUint8(v uint8)

	writeRaw(buff []byte)
}

type binaryNomsReader struct {
	buff   []byte
	offset uint32
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

func (b *binaryNomsReader) peekKind() NomsKind {
	return NomsKind(b.peekUint8())
}

func (b *binaryNomsReader) readKind() NomsKind {
	return NomsKind(b.readUint8())
}

func (b *binaryNomsReader) skipKind() {
	b.skipUint8()
}

func (b *binaryNomsReader) readCount() uint64 {
	v, count := binary.Uvarint(b.buff[b.offset:])
	b.offset += uint32(count)
	return v
}

func (b *binaryNomsReader) skipCount() {
	_, count := binary.Uvarint(b.buff[b.offset:])
	b.offset += uint32(count)
}

func (b *binaryNomsReader) readFloat(nbf *NomsBinFormat) Float {
	if isFormat_7_18(nbf) {
		// b.assertCanRead(binary.MaxVarintLen64 * 2)
		i, count := binary.Varint(b.buff[b.offset:])
		b.offset += uint32(count)
		exp, count2 := binary.Varint(b.buff[b.offset:])
		b.offset += uint32(count2)
		return Float(fracExpToFloat(i, int(exp)))
	} else {
		floatbits := binary.BigEndian.Uint64(b.buff[b.offset:])
		b.offset += 8
		return Float(math.Float64frombits(floatbits))
	}
}

func (b *binaryNomsReader) skipFloat(nbf *NomsBinFormat) {
	if isFormat_7_18(nbf) {
		_, count := binary.Varint(b.buff[b.offset:])
		b.offset += uint32(count)
		_, count2 := binary.Varint(b.buff[b.offset:])
		b.offset += uint32(count2)
	} else {
		b.offset += 8
	}
}

func (b *binaryNomsReader) skipInt() {
	_, count := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count)
}

func (b *binaryNomsReader) skipUint() {
	_, count := binary.Uvarint(b.buff[b.offset:])
	b.offset += uint32(count)
}

func (b *binaryNomsReader) readInt() Int {
	v, count := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count)
	return Int(v)
}

func (b *binaryNomsReader) readUint() Uint {
	v, count := binary.Uvarint(b.buff[b.offset:])
	b.offset += uint32(count)
	return Uint(v)
}

func (b *binaryNomsReader) readUUID() UUID {
	id := UUID{}
	copy(id[:uuidNumBytes], b.buff[b.offset:])
	b.offset += uuidNumBytes

	return id
}

func (b *binaryNomsReader) skipUUID() {
	b.offset += uuidNumBytes
}

func (b *binaryNomsReader) readBool() bool {
	return b.readUint8() == 1
}

func (b *binaryNomsReader) skipBool() {
	b.skipUint8()
}

func (b *binaryNomsReader) readString() string {
	size := uint32(b.readCount())

	v := string(b.buff[b.offset : b.offset+size])
	b.offset += size
	return v
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

func newBinaryNomsWriter() binaryNomsWriter {
	return binaryNomsWriter{make([]byte, initialBufferSize), 0}
}

func (b *binaryNomsWriter) data() []byte {
	return b.buff[0:b.offset]
}

func (b *binaryNomsWriter) reset() {
	b.offset = 0
}

func (b *binaryNomsWriter) ensureCapacity(n uint32) {
	length := uint32(len(b.buff))
	if b.offset+n <= length {
		return
	}

	old := b.buff

	for b.offset+n > length {
		length = length * 2
	}
	b.buff = make([]byte, length)

	copy(b.buff, old)
}

func (b *binaryNomsWriter) writeBytes(v []byte) {
	size := uint32(len(v))
	b.ensureCapacity(size)
	copy(b.buff[b.offset:], v)
	b.offset += size
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
	b.ensureCapacity(uint32(len(buff)))
	copy(b.buff[b.offset:], buff)
	b.offset += uint32(len(buff))
}
