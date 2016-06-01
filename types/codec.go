// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"crypto/sha1"
	"math"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
)

var initialBufferSize = 2048

func EncodeValue(v Value, vw ValueWriter) chunks.Chunk {
	w := &binaryNomsWriter{make([]byte, initialBufferSize, initialBufferSize), 0}
	enc := newValueEncoder(w, vw)
	enc.writeValue(v)

	c := chunks.NewChunk(w.data())
	if cacher, ok := v.(hashCacher); ok {
		assignHash(cacher, c.Hash())
	}

	return c
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vr ValueReader) Value {
	d.Chk.False(c.IsEmpty())
	data := c.Data()
	dec := newValueDecoder(&binaryNomsReader{data, 0}, vr)
	v := dec.readValue()

	if cacher, ok := v.(hashCacher); ok {
		assignHash(cacher, c.Hash())
	}

	return v
}

type nomsReader interface {
	readBytes() []byte
	readUint8() uint8
	readUint32() uint32
	readUint64() uint64
	readFloat64() float64
	readBool() bool
	readString() string
	readHash() hash.Hash
}

type nomsWriter interface {
	writeBytes(v []byte)
	writeUint8(v uint8)
	writeUint32(v uint32)
	writeUint64(v uint64)
	writeFloat64(v float64)
	writeBool(b bool)
	writeString(v string)
	writeHash(h hash.Hash)
}

type binaryNomsReader struct {
	buff   []byte
	offset uint32
}

func (b *binaryNomsReader) assertCanRead(n uint32) {
	if b.offset+n > uint32(len(b.buff)) {
		panic("unexpected end of input")
	}
}

func (b *binaryNomsReader) readBytes() []byte {
	size := b.readUint32()

	b.assertCanRead(size)
	buff := make([]byte, size, size)
	copy(buff, b.buff[b.offset:b.offset+size])
	b.offset += size
	return buff
}

func (b *binaryNomsReader) readUint8() uint8 {
	b.assertCanRead(1)
	v := uint8(b.buff[b.offset])
	b.offset++
	return v
}

func (b *binaryNomsReader) readUint32() uint32 {
	b.assertCanRead(4)
	v := uint32(b.buff[b.offset]) |
		uint32(b.buff[b.offset+1])<<8 |
		uint32(b.buff[b.offset+2])<<16 |
		uint32(b.buff[b.offset+3])<<24
	b.offset += 4
	return v
}

func (b *binaryNomsReader) readUint64() uint64 {
	b.assertCanRead(8)
	v := uint64(b.buff[b.offset]) |
		uint64(b.buff[b.offset+1])<<8 |
		uint64(b.buff[b.offset+2])<<16 |
		uint64(b.buff[b.offset+3])<<24 |
		uint64(b.buff[b.offset+4])<<32 |
		uint64(b.buff[b.offset+5])<<40 |
		uint64(b.buff[b.offset+6])<<48 |
		uint64(b.buff[b.offset+7])<<56
	b.offset += 8
	return v
}

func (b *binaryNomsReader) readFloat64() float64 {
	return math.Float64frombits(b.readUint64())
}

func (b *binaryNomsReader) readBool() bool {
	return b.readUint8() == 1
}

func (b *binaryNomsReader) readString() string {
	size := b.readUint32()

	b.assertCanRead(size)
	v := string(b.buff[b.offset : b.offset+size])
	b.offset += size
	return v
}

func (b *binaryNomsReader) readHash() hash.Hash {
	b.assertCanRead(sha1.Size)
	digest := hash.Sha1Digest{}
	copy(digest[:], b.buff[b.offset:b.offset+sha1.Size])
	b.offset += sha1.Size
	return hash.New(digest)
}

type binaryNomsWriter struct {
	buff   []byte
	offset uint32
}

func (b *binaryNomsWriter) data() []byte {
	return b.buff[0:b.offset]
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
	// Little-Endian: TODO: Need Big
	b.buff[b.offset] = byte(v)
	b.buff[b.offset+1] = byte(v >> 8)
	b.buff[b.offset+2] = byte(v >> 16)
	b.buff[b.offset+3] = byte(v >> 24)
	b.offset += 4
}

func (b *binaryNomsWriter) writeUint64(v uint64) {
	b.ensureCapacity(8)

	// Little-Endian: TODO: Need Big
	b.buff[b.offset] = byte(v)
	b.buff[b.offset+1] = byte(v >> 8)
	b.buff[b.offset+2] = byte(v >> 16)
	b.buff[b.offset+3] = byte(v >> 24)
	b.buff[b.offset+4] = byte(v >> 32)
	b.buff[b.offset+5] = byte(v >> 40)
	b.buff[b.offset+6] = byte(v >> 48)
	b.buff[b.offset+7] = byte(v >> 56)
	b.offset += 8
}

func (b *binaryNomsWriter) writeFloat64(v float64) {
	b.writeUint64(math.Float64bits(v))
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
	b.ensureCapacity(sha1.Size)
	digest := h.Digest()
	copy(b.buff[b.offset:], digest[:])
	b.offset += sha1.Size
}
