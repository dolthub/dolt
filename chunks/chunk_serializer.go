package chunks

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"io"
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

/*
  Chunk Serialization:
    Chunk 0
    Chunk 1
     ..
    Chunk N

  Chunk:
    Ref   // 20-byte sha1 hash
    Len   // 4-byte int
    Data  // len(Data) == Len
*/

// NewSerializer creates a serializer which is a ChunkSink. Put() chunks will be serialized to |writer|. Close() must be called when no more chunks will be serialized.
func NewSerializer(writer io.Writer) ChunkSink {
	s := &serializer{
		writer,
		make(chan Chunk, 64),
		make(chan struct{}),
	}

	go func() {
		for chunk := range s.chs {
			d.Chk.NotNil(chunk.Data)

			digest := chunk.Ref().Digest()
			n, err := io.Copy(s.writer, bytes.NewReader(digest[:]))
			d.Chk.NoError(err)
			d.Chk.Equal(int64(sha1.Size), n)

			// Because of chunking at higher levels, no chunk should never be more than 4GB
			chunkSize := uint32(len(chunk.Data()))
			err = binary.Write(s.writer, binary.BigEndian, chunkSize)
			d.Chk.NoError(err)

			n, err = io.Copy(s.writer, bytes.NewReader(chunk.Data()))
			d.Chk.NoError(err)
			d.Chk.Equal(uint32(n), chunkSize)
		}

		s.done <- struct{}{}
	}()

	return s
}

type serializer struct {
	writer io.Writer
	chs    chan Chunk
	done   chan struct{}
}

func (sz *serializer) Put(c Chunk) {
	sz.chs <- c
}

func (sz *serializer) PutMany(chunks []Chunk) (e BackpressureError) {
	for _, c := range chunks {
		sz.chs <- c
	}
	return
}

func (sz *serializer) Close() error {
	close(sz.chs)
	<-sz.done
	return nil
}

// Deserialize reads off of |reader| until EOF, sending chunks to |cs|. If |rateLimit| is non-nill, concurrency will be limited to the available capacity of the channel.
func Deserialize(reader io.Reader, cs ChunkSink, rateLimit chan struct{}) {
	wg := sync.WaitGroup{}

	for {
		c := deserializeChunk(reader)
		if c.IsEmpty() {
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
func DeserializeToChan(reader io.Reader, chunkChan chan<- Chunk) {
	for {
		c := deserializeChunk(reader)
		if c.IsEmpty() {
			break
		}
		chunkChan <- c
	}
	close(chunkChan)
}

func deserializeChunk(reader io.Reader) Chunk {
	digest := ref.Sha1Digest{}
	n, err := io.ReadFull(reader, digest[:])
	if err == io.EOF {
		return EmptyChunk
	}
	d.Chk.NoError(err)
	d.Chk.Equal(int(sha1.Size), n)
	r := ref.New(digest)

	chunkSize := uint32(0)
	err = binary.Read(reader, binary.BigEndian, &chunkSize)
	d.Chk.NoError(err)

	w := NewChunkWriter()
	n2, err := io.CopyN(w, reader, int64(chunkSize))
	d.Chk.NoError(err)
	d.Chk.Equal(int64(chunkSize), n2)
	c := w.Chunk()
	d.Chk.Equal(r, c.Ref())
	return c
}
