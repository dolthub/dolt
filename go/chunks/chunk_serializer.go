// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

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

	digest := chunk.Hash().Digest()
	n, err := io.Copy(writer, bytes.NewReader(digest[:]))
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

// Deserialize reads off of |reader| until EOF, sending chunks to |cs|. If |rateLimit| is non-nill, concurrency will be limited to the available capacity of the channel.
func Deserialize(reader io.Reader, cs ChunkSink, rateLimit chan struct{}) {
	wg := sync.WaitGroup{}

	for {
		c, success := deserializeChunk(reader)
		if !success {
			break
		}

		wg.Add(1)
		if rateLimit != nil {
			rateLimit <- struct{}{}
		}
		go func() {
			cs.Put(c)
			wg.Done()
			if rateLimit != nil {
				<-rateLimit
			}
		}()
	}

	wg.Wait()
}

// DeserializeToChan reads off of |reader| until EOF, sending chunks to chunkChan in the order they are read.
func DeserializeToChan(reader io.Reader, chunkChan chan<- *Chunk) {
	for {
		c, success := deserializeChunk(reader)
		if !success {
			break
		}
		chunkChan <- &c
	}
	close(chunkChan)
}

func deserializeChunk(reader io.Reader) (Chunk, bool) {
	digest := hash.Digest{}
	n, err := io.ReadFull(reader, digest[:])
	if err == io.EOF {
		return EmptyChunk, false
	}
	d.Chk.NoError(err)
	d.PanicIfFalse(int(hash.ByteLen) == n)
	h := hash.New(digest)

	chunkSize := uint32(0)
	err = binary.Read(reader, binary.BigEndian, &chunkSize)
	d.Chk.NoError(err)

	w := NewChunkWriter()
	n2, err := io.CopyN(w, reader, int64(chunkSize))
	d.Chk.NoError(err)
	d.PanicIfFalse(int64(chunkSize) == n2)
	c := w.Chunk()
	d.PanicIfFalse(h == c.Hash(), "%s != %s", h, c.Hash())
	return c, true
}
