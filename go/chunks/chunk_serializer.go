// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

/*
  Chunk Serialization:
    Chunk 0
    Chunk 1
     ..
    Chunk N

  Chunk:
    Hash  // 20-byte hash
    Len   // 4-byte int
    Data  // len(Data) == Len
*/

// Serialize a single Chunk to writer.
func Serialize(chunk Chunk, writer io.Writer) {
	d.PanicIfFalse(chunk.data != nil)

	h := chunk.Hash()
	n, err := io.Copy(writer, bytes.NewReader(h[:]))
	d.Chk.NoError(err)
	d.PanicIfFalse(int64(hash.ByteLen) == n)

	// Because of chunking at higher levels, no chunk should never be more than 4GB
	chunkSize := uint32(len(chunk.Data()))
	err = binary.Write(writer, binary.BigEndian, chunkSize)
	d.Chk.NoError(err)

	n, err = io.Copy(writer, bytes.NewReader(chunk.Data()))
	d.Chk.NoError(err)
	d.PanicIfFalse(uint32(n) == chunkSize)
}

// Deserialize reads off of |reader| until EOF, sending chunks to
// chunkChan in the order they are read. Objects sent over chunkChan are
// *Chunk.
func Deserialize(reader io.Reader, chunkChan chan<- *Chunk) (err error) {
	for {
		var c Chunk
		c, err = deserializeChunk(reader)
		if err != nil {
			break
		}
		d.Chk.NotEqual(EmptyChunk.Hash(), c.Hash())
		chunkChan <- &c
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func deserializeChunk(reader io.Reader) (Chunk, error) {
	h := hash.Hash{}
	n, err := io.ReadFull(reader, h[:])
	if err != nil {
		return EmptyChunk, err
	}
	d.PanicIfFalse(int(hash.ByteLen) == n)

	chunkSize := uint32(0)
	if err = binary.Read(reader, binary.BigEndian, &chunkSize); err != nil {
		return EmptyChunk, err
	}

	data := make([]byte, int(chunkSize))
	if n, err = io.ReadFull(reader, data); err != nil {
		return EmptyChunk, err
	}
	d.PanicIfFalse(int(chunkSize) == n)
	c := NewChunk(data)
	if h != c.Hash() {
		d.Panic("%s != %s", h, c.Hash().String())
	}
	return c, nil
}
