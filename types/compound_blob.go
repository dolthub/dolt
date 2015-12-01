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
	ref *ref.Ref
	cs  chunks.ChunkStore
}

var typeForCompoundBlob = MakeCompoundType(MetaSequenceKind, MakePrimitiveType(BlobKind))

func newCompoundBlob(tuples metaSequenceData, cs chunks.ChunkStore) compoundBlob {
	return buildCompoundBlob(tuples, typeForCompoundBlob, cs).(compoundBlob)
}

func buildCompoundBlob(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	d.Chk.True(t.Equals(typeForCompoundBlob))
	return compoundBlob{metaSequenceObject{tuples, typeForCompoundBlob}, &ref.Ref{}, cs}
}

func init() {
	registerMetaValue(BlobKind, buildCompoundBlob)
}

func (cb compoundBlob) Reader() io.ReadSeeker {
	length := uint64(cb.lastTuple().value.(UInt64))
	cursor, v := newMetaSequenceCursor(cb, cb.cs)
	reader := v.(blobLeaf).Reader()
	return &compoundBlobReader{cursor: cursor, currentReader: reader, length: length, cs: cb.cs}
}

func (cb compoundBlob) Equals(other Value) bool {
	return other != nil && cb.t.Equals(other.Type()) && cb.Ref() == other.Ref()
}

func (cb compoundBlob) Ref() ref.Ref {
	return EnsureRef(cb.ref, cb)
}

func (cb compoundBlob) Len() uint64 {
	return cb.tuples[len(cb.tuples)-1].uint64Value()
}

type compoundBlobReader struct {
	cursor                          *metaSequenceCursor
	currentReader                   io.ReadSeeker
	chunkStart, chunkOffset, length uint64
	cs                              chunks.ChunkSource
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

	chunkStart := cbr.cursor.seek(func(v, parent Value) bool {
		d.Chk.NotNil(v)
		d.Chk.NotNil(parent)

		return seekAbs < uint64(parent.(UInt64))+uint64(v.(UInt64))
	}, func(parent, prev, current Value) Value {
		pv := uint64(0)
		if prev != nil {
			pv = uint64(prev.(UInt64))
		}

		return UInt64(uint64(parent.(UInt64)) + pv)
	}, UInt64(0))

	cbr.chunkStart = uint64(chunkStart.(UInt64))
	cbr.chunkOffset = seekAbs - cbr.chunkStart
	cbr.currentReader = nil
	return int64(seekAbs), nil
}

func (cbr *compoundBlobReader) updateReader() {
	cbr.currentReader = cbr.cursor.currentVal().(blobLeaf).Reader()
	cbr.currentReader.Seek(int64(cbr.chunkOffset), 0)
}
