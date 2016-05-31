// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
)

func EncodeValue(v Value, vw ValueWriter) chunks.Chunk {
	buff := &bytes.Buffer{}
	enc := newValueEncoder(binaryNomsWriter{buff}, vw)
	enc.writeValue(v)
	return chunks.NewChunk(buff.Bytes())
}

// DecodeValue decodes a value from a chunk source. It is an error to provide an empty chunk.
func DecodeValue(c chunks.Chunk, vr ValueReader) Value {
	d.Chk.False(c.IsEmpty())
	data := c.Data()
	dec := newValueDecoder(binaryNomsReader{bytes.NewReader(data), data}, vr)
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
	*bytes.Reader
	data []byte
}

func (b binaryNomsReader) pos() int64 {
	return b.Size() - int64(b.Len())
}

func (b binaryNomsReader) readBytes() []byte {
	size := b.readUint32()
	buff := bytes.Buffer{}
	n, err := io.CopyN(&buff, b, int64(size))
	d.Chk.NoError(err)
	d.Chk.True(int64(size) == n)
	return buff.Bytes()
}

func (b binaryNomsReader) readUint8() uint8 {
	v, err := b.ReadByte()
	d.Chk.NoError(err)
	return uint8(v)
}

func (b binaryNomsReader) readUint32() uint32 {
	v := uint32(0)
	err := binary.Read(b, binary.LittleEndian, &v)
	d.Chk.NoError(err)
	return v
}

func (b binaryNomsReader) readUint64() uint64 {
	v := uint64(0)
	err := binary.Read(b, binary.LittleEndian, &v)
	d.Chk.NoError(err)
	return v
}

func (b binaryNomsReader) readFloat64() float64 {
	v := float64(0)
	err := binary.Read(b, binary.LittleEndian, &v)
	d.Chk.NoError(err)
	return v
}

func (b binaryNomsReader) readBool() bool {
	v, err := b.ReadByte()
	d.Chk.NoError(err)
	return v == uint8(1)
}

func (b binaryNomsReader) readString() string {
	size := b.readUint32()
	pos := b.pos()
	end := pos + int64(size)
	b.Seek(end, 0) // Advance |size| bytes
	return string(b.data[pos:end])
}

func (b binaryNomsReader) readHash() hash.Hash {
	digest := hash.Sha1Digest{}
	n, err := io.ReadFull(b, digest[:])
	d.Chk.NoError(err)
	d.Chk.True(int(sha1.Size) == n)
	return hash.New(digest)
}

type binaryNomsWriter struct {
	*bytes.Buffer
}

func (b binaryNomsWriter) writeBytes(v []byte) {
	size := uint32(len(v))
	b.writeUint32(size)
	n, err := io.Copy(b, bytes.NewReader(v))
	d.Chk.NoError(err)
	d.Chk.True(int(n) == len(v))
}

func (b binaryNomsWriter) writeUint8(v uint8) {
	b.WriteByte(v)
}

func (b binaryNomsWriter) writeUint32(v uint32) {
	err := binary.Write(b, binary.LittleEndian, v)
	d.Chk.NoError(err)
}

func (b binaryNomsWriter) writeUint64(v uint64) {
	err := binary.Write(b, binary.LittleEndian, v)
	d.Chk.NoError(err)
}

func (b binaryNomsWriter) writeFloat64(v float64) {
	err := binary.Write(b, binary.LittleEndian, v)
	d.Chk.NoError(err)
}

func (b binaryNomsWriter) writeBool(v bool) {
	if v {
		b.WriteByte(uint8(1))
	} else {
		b.WriteByte(uint8(0))
	}
}

func (b binaryNomsWriter) writeString(v string) {
	size := uint32(len(v))
	b.writeUint32(size)
	b.WriteString(v)
}

func (b binaryNomsWriter) writeHash(h hash.Hash) {
	digest := h.Digest()
	n, err := io.Copy(b, bytes.NewReader(digest[:]))
	d.Chk.NoError(err)
	d.Chk.True(int64(sha1.Size) == n)
}
