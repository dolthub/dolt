package types

import (
	"bytes"
	"errors"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	blobPattern = uint32(1<<11 - 1) // Avg Chunk Size of 2k

	// The window size to use for computing the rolling hash.
	blobWindowSize = 64
)

var RefOfBlobType = MakeRefType(BlobType)

// Blob represents a list of Blobs.
type Blob struct {
	seq indexedSequence
	ref *ref.Ref
}

func newBlob(seq indexedSequence) Blob {
	return Blob{seq, &ref.Ref{}}
}

func NewEmptyBlob() Blob {
	return Blob{newBlobLeafSequence(nil, []byte{}), &ref.Ref{}}
}

// BUG 155 - Should provide Write... Maybe even have Blob implement ReadWriteSeeker
func (b Blob) Reader() io.ReadSeeker {
	cursor := newCursorAtIndex(b.seq, 0)
	return &BlobReader{b.seq, cursor, nil, 0}
}

func (b Blob) Equals(other Value) bool {
	return other != nil && b.Ref() == other.Ref()
}

func (b Blob) Less(other Value) bool {
	return valueLess(b, other)
}

func (b Blob) Ref() ref.Ref {
	return EnsureRef(b.ref, b)
}

func (b Blob) Len() uint64 {
	return b.seq.numLeaves()
}

func (b Blob) Empty() bool {
	return b.Len() == 0
}

func (b Blob) Chunks() []Ref {
	return b.seq.Chunks()
}

func (b Blob) ChildValues() []Value {
	return []Value{}
}

func (b Blob) Type() *Type {
	return b.seq.Type()
}

func (b Blob) sequence() sequence {
	return b.seq
}

type BlobReader struct {
	seq           indexedSequence
	cursor        *sequenceCursor
	currentReader io.ReadSeeker
	pos           uint64
}

func (cbr *BlobReader) Read(p []byte) (n int, err error) {
	if cbr.currentReader == nil {
		cbr.updateReader()
	}

	n, err = cbr.currentReader.Read(p)
	for i := 0; i < n; i++ {
		cbr.pos++
		cbr.cursor.advance()
	}
	if err == io.EOF && cbr.cursor.idx < cbr.cursor.seq.seqLen() {
		cbr.currentReader = nil
		err = nil
	}

	return
}

func (cbr *BlobReader) Seek(offset int64, whence int) (int64, error) {
	abs := int64(cbr.pos)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(cbr.seq.numLeaves()) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	cbr.pos = uint64(abs)
	cbr.cursor = newCursorAtIndex(cbr.seq, cbr.pos)
	cbr.currentReader = nil
	return abs, nil
}

func (cbr *BlobReader) updateReader() {
	cbr.currentReader = bytes.NewReader(cbr.cursor.seq.(blobLeafSequence).data)
	cbr.currentReader.Seek(int64(cbr.cursor.idx), 0)
}

func newBlobLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(blobWindowSize, 1, blobPattern, func(item sequenceItem) []byte {
		return []byte{item.(byte)}
	})
}

func newBlobLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, Collection) {
		buff := make([]byte, len(items))

		for i, v := range items {
			buff[i] = v.(byte)
		}

		blob := newBlob(newBlobLeafSequence(vr, buff))
		return newMetaTuple(Number(len(buff)), blob, NewRef(blob), uint64(len(buff))), blob
	}
}

func NewBlob(r io.Reader) Blob {
	seq := newEmptySequenceChunker(newBlobLeafChunkFn(nil), newIndexedMetaSequenceChunkFn(BlobKind, nil, nil), newBlobLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
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
