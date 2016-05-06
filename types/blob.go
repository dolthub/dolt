package types

import (
	"io"

	"github.com/attic-labs/noms/d"
)

const (
	blobPattern = uint32(1<<11 - 1) // Avg Chunk Size of 2k

	// The window size to use for computing the rolling hash.
	blobWindowSize = 64
)

var RefOfBlobType = MakeRefType(BlobType)

type Blob interface {
	Collection
	// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
	Reader() io.ReadSeeker
}

func NewEmptyBlob() Blob {
	return newBlobLeaf([]byte{}).(Blob)
}

func newBlobLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(blobWindowSize, 1, blobPattern, func(item sequenceItem) []byte {
		return []byte{item.(byte)}
	})
}

func newBlobLeafChunkFn() makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		buff := make([]byte, len(items))

		for i, v := range items {
			buff[i] = v.(byte)
		}

		leaf := newBlobLeaf(buff)
		return newMetaTuple(Number(len(buff)), leaf, NewTypedRefFromValue(leaf), uint64(len(buff))), leaf
	}
}

func NewBlob(r io.Reader) Blob {
	seq := newEmptySequenceChunker(newBlobLeafChunkFn(), newIndexedMetaSequenceChunkFn(BlobType, nil, nil), newBlobLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
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
	return seq.Done().(Blob)
}
