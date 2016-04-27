package types

import (
	"io"

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

var typeForBlob = BlobType

type Blob interface {
	Value
	Len() uint64
	// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
	Reader() io.ReadSeeker
}

func NewEmptyBlob() Blob {
	return newBlobLeaf([]byte{})
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
		return newMetaTuple(Number(len(buff)), leaf, Ref{}, uint64(len(buff))), leaf
	}
}

func NewBlob(r io.Reader) Blob {
	seq := newEmptySequenceChunker(newBlobLeafChunkFn(), newIndexedMetaSequenceChunkFn(typeForBlob, nil, nil), newBlobLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
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
