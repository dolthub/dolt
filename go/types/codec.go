// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const initialBufferSize = 2048

type valueBytes interface {
	valueBytes() []byte
}

func EncodeValue(v Value) chunks.Chunk {
	switch v := v.(type) {
	case valueBytes:
		return chunks.NewChunk(v.valueBytes())
	case *Type:
		w := newBinaryNomsWriter()
		v.writeTo(&w)
		return chunks.NewChunk(w.data())
	}

	panic("unreachable")
}

func DecodeFromBytes(data []byte, vrw ValueReadWriter) Value {
	dec := newValueDecoder(data, vrw)
	v := dec.readValue()
	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v
}

func decodeFromBytesWithValidation(data []byte, vrw ValueReadWriter) Value {
	r := binaryNomsReader{data, 0}
	dec := newValueDecoderWithValidation(r, vrw)
	v := dec.readValue()
	d.PanicIfFalse(dec.pos() == uint32(len(data)))
	return v
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vrw ValueReadWriter) Value {
	d.PanicIfTrue(c.IsEmpty())
	return DecodeFromBytes(c.Data(), vrw)
}

type nomsWriter interface {
	writeBool(b bool)
	writeBytes(v []byte)
	writeCount(count uint64)
	writeHash(h hash.Hash)
	writeNumber(v Number)
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

func (b *binaryNomsReader) readNumber() Number {
	// b.assertCanRead(binary.MaxVarintLen64 * 2)
	i, count := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count)
	exp, count2 := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count2)
	return Number(fracExpToFloat(i, int(exp)))
}

func (b *binaryNomsReader) skipNumber() {
	_, count := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count)
	_, count2 := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count2)
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
	b.buff = make([]byte, length, length)

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

func (b *binaryNomsWriter) writeNumber(v Number) {
	b.ensureCapacity(binary.MaxVarintLen64 * 2)
	i, exp := float64ToIntExp(float64(v))
	count := binary.PutVarint(b.buff[b.offset:], i)
	b.offset += uint32(count)
	count = binary.PutVarint(b.buff[b.offset:], int64(exp))
	b.offset += uint32(count)
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
