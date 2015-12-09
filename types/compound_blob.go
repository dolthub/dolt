package types

import (
	"errors"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	metaSequenceObject
	length uint64
	ref    *ref.Ref
	cs     chunks.ChunkStore
}

func newCompoundBlob(tuples metaSequenceData, cs chunks.ChunkStore) compoundBlob {
	return buildCompoundBlob(tuples, typeForBlob, cs).(compoundBlob)
}

func buildCompoundBlob(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	d.Chk.True(t.Equals(typeForBlob))
	return compoundBlob{metaSequenceObject{tuples, typeForBlob}, tuples.uint64ValuesSum(), &ref.Ref{}, cs}
}

func init() {
	registerMetaValue(BlobKind, buildCompoundBlob)
}

func (cb compoundBlob) Reader() io.ReadSeeker {
	cursor, v := newMetaSequenceCursor(cb, cb.cs)
	reader := v.(blobLeaf).Reader()
	return &compoundBlobReader{cursor: cursor, currentReader: reader, length: cb.Len(), cs: cb.cs}
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

type compoundBlobReader struct {
	cursor                          *sequenceCursor
	currentReader                   io.ReadSeeker
	chunkStart, chunkOffset, length uint64
	cs                              chunks.ChunkStore
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
	if cbr.currentReader == nil {
		cbr.updateReader()
	}

	n, err = cbr.currentReader.Read(p)
	if n > 0 || err != io.EOF {
		if err == io.EOF {
			err = nil
		}
		cbr.chunkOffset += uint64(n)
		return
	}

	if !cbr.cursor.advance() {
		return 0, io.EOF
	}

	cbr.chunkStart = cbr.chunkStart + cbr.chunkOffset
	cbr.chunkOffset = 0
	cbr.currentReader = nil
	return cbr.Read(p)
}

func (cbr *compoundBlobReader) Seek(offset int64, whence int) (int64, error) {
	abs := int64(cbr.chunkStart) + int64(cbr.chunkOffset)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(cbr.length) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	seekAbs := uint64(abs)

	chunkStart := cbr.cursor.seekLinear(func(carry interface{}, mt sequenceItem) (bool, interface{}) {
		offset := carry.(uint64) + mt.(metaTuple).uint64Value()
		return seekAbs < offset, offset
	}, uint64(0))

	cbr.chunkStart = chunkStart.(uint64)
	cbr.chunkOffset = seekAbs - cbr.chunkStart
	cbr.currentReader = nil
	return abs, nil
}

func (cbr *compoundBlobReader) updateReader() {
	cbr.currentReader = readMetaTupleValue(cbr.cursor.current(), cbr.cs).(blobLeaf).Reader()
	cbr.currentReader.Seek(int64(cbr.chunkOffset), 0)
}
