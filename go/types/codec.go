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

func EncodeValue(v Value, vw ValueWriter) chunks.Chunk {
	w := newBinaryNomsWriter()
	enc := newValueEncoder(w, vw)
	enc.writeValue(v)

	c := chunks.NewChunk(w.data())
	if cacher, ok := v.(hashCacher); ok {
		assignHash(cacher, c.Hash())
	}

	return c
}

func DecodeFromBytes(data []byte, vr ValueReader, tc *TypeCache) Value {
	tc.Lock()
	defer tc.Unlock()
	br := &binaryNomsReader{data, 0}
	dec := newValueDecoder(br, vr, tc)
	v := dec.readValue()
	d.PanicIfFalse(br.pos() == uint32(len(data)))
	return v
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vr ValueReader) Value {
	d.PanicIfTrue(c.IsEmpty())
	v := DecodeFromBytes(c.Data(), vr, staticTypeCache)
	if cacher, ok := v.(hashCacher); ok {
		assignHash(cacher, c.Hash())
	}

	return v
}

type nomsReader interface {
	pos() uint32
	seek(pos uint32)
	readBytes() []byte
	readUint8() uint8
	readUint32() uint32
	readUint64() uint64
	readNumber() Number
	readBool() bool
	readString() string
	readIdent(tc *TypeCache) uint32
	readHash() hash.Hash
}

type nomsWriter interface {
	writeBytes(v []byte)
	writeUint8(v uint8)
	writeUint32(v uint32)
	writeUint64(v uint64)
	writeNumber(v Number)
	writeBool(b bool)
	writeString(v string)
	writeHash(h hash.Hash)
	appendType(t *Type)
}

type binaryNomsReader struct {
	buff   []byte
	offset uint32
}

func (b *binaryNomsReader) pos() uint32 {
	return b.offset
}

func (b *binaryNomsReader) seek(pos uint32) {
	b.offset = pos
}

func (b *binaryNomsReader) readBytes() []byte {
	size := b.readUint32()

	buff := make([]byte, size, size)
	copy(buff, b.buff[b.offset:b.offset+size])
	b.offset += size
	return buff
}

func (b *binaryNomsReader) readUint8() uint8 {
	v := uint8(b.buff[b.offset])
	b.offset++
	return v
}

func (b *binaryNomsReader) readUint32() uint32 {
	// Big-Endian
	v := uint32(b.buff[b.offset])<<24 |
		uint32(b.buff[b.offset+1])<<16 |
		uint32(b.buff[b.offset+2])<<8 |
		uint32(b.buff[b.offset+3])
	b.offset += 4
	return v
}

func (b *binaryNomsReader) readUint64() uint64 {
	// Big-Endian
	v := uint64(b.buff[b.offset])<<56 |
		uint64(b.buff[b.offset+1])<<48 |
		uint64(b.buff[b.offset+2])<<40 |
		uint64(b.buff[b.offset+3])<<32 |
		uint64(b.buff[b.offset+4])<<24 |
		uint64(b.buff[b.offset+5])<<16 |
		uint64(b.buff[b.offset+6])<<8 |
		uint64(b.buff[b.offset+7])
	b.offset += 8
	return v
}

func (b *binaryNomsReader) readNumber() Number {
	// b.assertCanRead(binary.MaxVarintLen64 * 2)
	i, count := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count)
	exp, count2 := binary.Varint(b.buff[b.offset:])
	b.offset += uint32(count2)
	return Number(intExpToFloat64(i, int(exp)))
}

func (b *binaryNomsReader) readBool() bool {
	return b.readUint8() == 1
}

func (b *binaryNomsReader) readString() string {
	size := b.readUint32()

	v := string(b.buff[b.offset : b.offset+size])
	b.offset += size
	return v
}

// Note: It's somewhat of a layering violation that a nomsReaders knows about a TypeCache. The reason why the code is structured this way is that the go compiler can stack-allocate the string which is created from the byte slice, which is a fairly large perf gain.
func (b *binaryNomsReader) readIdent(tc *TypeCache) uint32 {
	size := b.readUint32()
	id, ok := tc.identTable.entries[string(b.buff[b.offset:b.offset+size])]
	if !ok {
		id = tc.identTable.GetId(string(b.buff[b.offset : b.offset+size]))
	}

	b.offset += size
	return id
}

func (b *binaryNomsReader) readHash() hash.Hash {
	digest := hash.Digest{}
	copy(digest[:], b.buff[b.offset:b.offset+hash.ByteLen])
	b.offset += hash.ByteLen
	return hash.New(digest)
}

type binaryNomsWriter struct {
	buff   []byte
	offset uint32
}

func newBinaryNomsWriter() *binaryNomsWriter {
	return &binaryNomsWriter{make([]byte, initialBufferSize, initialBufferSize), 0}
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
	b.writeUint32(size)

	b.ensureCapacity(size)
	copy(b.buff[b.offset:], v)
	b.offset += size
}

func (b *binaryNomsWriter) writeUint8(v uint8) {
	b.ensureCapacity(1)
	b.buff[b.offset] = byte(v)
	b.offset++
}

func (b *binaryNomsWriter) writeUint32(v uint32) {
	b.ensureCapacity(4)
	// Big-Endian
	b.buff[b.offset] = byte(v >> 24)
	b.buff[b.offset+1] = byte(v >> 16)
	b.buff[b.offset+2] = byte(v >> 8)
	b.buff[b.offset+3] = byte(v)
	b.offset += 4
}

func (b *binaryNomsWriter) writeUint64(v uint64) {
	b.ensureCapacity(8)
	// Big-Endian
	b.buff[b.offset] = byte(v >> 56)
	b.buff[b.offset+1] = byte(v >> 48)
	b.buff[b.offset+2] = byte(v >> 40)
	b.buff[b.offset+3] = byte(v >> 32)
	b.buff[b.offset+4] = byte(v >> 24)
	b.buff[b.offset+5] = byte(v >> 16)
	b.buff[b.offset+6] = byte(v >> 8)
	b.buff[b.offset+7] = byte(v)
	b.offset += 8
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
	b.writeUint32(size)

	b.ensureCapacity(size)
	copy(b.buff[b.offset:], v)
	b.offset += size
}

func (b *binaryNomsWriter) writeHash(h hash.Hash) {
	b.ensureCapacity(hash.ByteLen)
	digest := h.Digest()
	copy(b.buff[b.offset:], digest[:])
	b.offset += hash.ByteLen
}

func (b *binaryNomsWriter) appendType(t *Type) {
	data := t.serialization
	size := uint32(len(data))
	b.ensureCapacity(size)

	copy(b.buff[b.offset:], data)
	b.offset += size
}
