package types

import (
	"io"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
)

const (
	// 12 bits leads to an average size of 4k
	// 13 bits leads to an average size of 8k
	// 14 bits leads to an average size of 16k
	blobPattern = uint32(1<<13 - 1)

	// The window size to use for computing the rolling hash.
	blobWindowSize = 64
)

var typeForBlob = MakePrimitiveType(BlobKind)

type Blob interface {
	Value
	Len() uint64
	// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
	Reader() io.ReadSeeker
}

func NewEmptyBlob() Blob {
	return newBlobLeaf([]byte{})
}

func NewMemoryBlob(r io.Reader) Blob {
	return NewBlob(r, chunks.NewMemoryStore())
}

func blobLeafIsBoundaryFn() isBoundaryFn {
	h := buzhash.NewBuzHash(blobWindowSize)

	return func(item sequenceItem) bool {
		b, ok := item.(byte)
		d.Chk.True(ok)
		return h.HashByte(b)&blobPattern == blobPattern
	}
}

func newBlobLeafChunkFn(cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, interface{}) {
		buff := make([]byte, len(items))

		for i, v := range items {
			b, ok := v.(byte)
			d.Chk.True(ok)
			buff[i] = b
		}

		leaf := newBlobLeaf(buff)
		ref := WriteValue(leaf, cs)
		return metaTuple{ref, UInt64(uint64(len(buff)))}, leaf
	}
}

func NewBlob(r io.Reader, cs chunks.ChunkStore) Blob {
	seq := newSequenceChunker(newBlobLeafChunkFn(cs), newMetaSequenceChunkFn(typeForCompoundBlob, cs), blobLeafIsBoundaryFn(), metaSequenceIsBoundaryFn())
	buf := []byte{0}
	for {
		n, err := r.Read(buf)
		d.Chk.True(n <= 1)
		if n == 1 {
			seq.Append(buf[0])
		}
		if err != nil {
			d.Chk.Equal(io.EOF, err)
			break
		}
	}
	_, blob := seq.Done()
	return blob.(Blob)
}
