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

var typeRefForBlob = MakePrimitiveTypeRef(BlobKind)

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

func blobLeafIsBoundary() isBoundaryFn {
	h := buzhash.NewBuzHash(blobWindowSize)

	return func(item sequenceItem) bool {
		b, ok := item.(byte)
		d.Chk.True(ok)
		return h.HashByte(b)&blobPattern == blobPattern
	}
}

func newBlobLeafChunk(cs chunks.ChunkStore) makeChunkFn {
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

func compoundBlobIsBoundary() isBoundaryFn {
	h := buzhash.NewBuzHash(objectWindowSize)

	return func(item sequenceItem) bool {
		mt, ok := item.(metaTuple)
		d.Chk.True(ok)
		digest := mt.ref.Digest()
		_, err := h.Write(digest[:])
		d.Chk.NoError(err)
		return h.Sum32()&objectPattern == objectPattern
	}
}

func newCompoundBlobChunk(cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, interface{}) {
		tuples := make(metaSequenceData, len(items))
		offsetSum := uint64(0)

		for i, v := range items {
			mt, ok := v.(metaTuple)
			d.Chk.True(ok)
			offsetSum += mt.uint64Value()
			mt.value = UInt64(offsetSum)
			tuples[i] = mt
		}

		meta := newCompoundBlob(tuples, cs)
		ref := WriteValue(meta, cs)
		return metaTuple{ref, UInt64(offsetSum)}, meta
	}
}

func NewBlob(r io.Reader, cs chunks.ChunkStore) Blob {
	seq := newSequenceChunker(newBlobLeafChunk(cs), newCompoundBlobChunk(cs), blobLeafIsBoundary(), compoundBlobIsBoundary())
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
