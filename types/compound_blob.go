package types

import (
	"errors"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	indexedMetaSequence
	length uint64
	ref    *ref.Ref
}

func newCompoundBlob(tuples metaSequenceData, vr ValueReader) compoundBlob {
	return buildCompoundBlob(tuples, BlobType, vr).(compoundBlob)
}

func buildCompoundBlob(tuples metaSequenceData, t *Type, vr ValueReader) metaSequence {
	d.Chk.True(t.Equals(BlobType))
	return compoundBlob{
		indexedMetaSequence{
			metaSequenceObject{tuples, BlobType, vr},
			computeIndexedSequenceOffsets(tuples),
		},
		tuples.uint64ValuesSum(),
		&ref.Ref{},
	}
}

func init() {
	registerMetaValue(BlobKind, buildCompoundBlob)
}

func (cb compoundBlob) Reader() io.ReadSeeker {
	cursor := newCursorAtIndex(cb, 0)
	return &compoundBlobReader{cb, cursor, nil, 0, cb.vr}
}

func (cb compoundBlob) Equals(other Value) bool {
	return other != nil && cb.t.Equals(other.Type()) && cb.Ref() == other.Ref()
}

func (cb compoundBlob) Ref() ref.Ref {
	return EnsureRef(cb.ref, cb)
}

func (cb compoundBlob) Len() uint64 {
	return cb.length
}

func (cb compoundBlob) Empty() bool {
	return cb.length == 0
}

type compoundBlobReader struct {
	blob          Blob
	cursor        *sequenceCursor
	currentReader io.ReadSeeker
	pos           uint64
	vr            ValueReader
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
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

func (cbr *compoundBlobReader) Seek(offset int64, whence int) (int64, error) {
	abs := int64(cbr.pos)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(cbr.blob.Len()) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	cbr.pos = uint64(abs)
	cbr.cursor = newCursorAtIndex(cbr.blob.(indexedSequence), cbr.pos)
	cbr.currentReader = nil
	return abs, nil
}

func (cbr *compoundBlobReader) updateReader() {
	cbr.currentReader = cbr.cursor.seq.(blobLeaf).Reader()
	cbr.currentReader.Seek(int64(cbr.cursor.idx), 0)
}
