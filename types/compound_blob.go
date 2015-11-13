package types

import (
	"crypto/sha1"
	"errors"
	"io"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	objectWindowSize = 8 * sha1.Size
	objectPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	tuples metaSequenceData
	ref    *ref.Ref
	cs     chunks.ChunkSource
}

var typeRefForCompoundBlob = MakeCompoundTypeRef(MetaSequenceKind, MakePrimitiveTypeRef(BlobKind))

func newCompoundBlob(tuples metaSequenceData, cs chunks.ChunkSource) compoundBlob {
	return buildCompoundBlob(tuples, typeRefForCompoundBlob, cs).(compoundBlob)
}

func buildCompoundBlob(tuples metaSequenceData, t Type, cs chunks.ChunkSource) Value {
	d.Chk.True(t.Equals(typeRefForCompoundBlob))
	return compoundBlob{tuples, &ref.Ref{}, cs}
}

func getSequenceData(v Value) metaSequenceData {
	return v.(compoundBlob).tuples
}

func init() {
	registerMetaValue(BlobKind, buildCompoundBlob, getSequenceData)
}

func (cb compoundBlob) Reader() io.ReadSeeker {
	length := uint64(cb.lastTuple().value.(UInt64))
	return &compoundBlobReader{cursor: newMetaSequenceCursor(cb, cb.cs), length: length, cs: cb.cs}
}

// MetaSequence
func (cb compoundBlob) tupleAt(idx int) metaTuple {
	return cb.tuples[idx]
}

func (cb compoundBlob) tupleCount() int {
	return len(cb.tuples)
}

func (cb compoundBlob) lastTuple() metaTuple {
	return cb.tuples[cb.tupleCount()-1]
}

func (cb compoundBlob) Equals(other Value) bool {
	return other != nil && typeRefForCompoundBlob.Equals(other.Type()) && cb.Ref() == other.Ref()
}

func (cb compoundBlob) Ref() ref.Ref {
	return EnsureRef(cb.ref, cb)
}

func (cb compoundBlob) Type() Type {
	return typeRefForCompoundBlob
}

func (cb compoundBlob) ChildValues() []Value {
	res := make([]Value, len(cb.tuples))
	for i, t := range cb.tuples {
		res[i] = NewRefOfBlob(t.ref)
	}
	return res
}

func (cb compoundBlob) Chunks() (chunks []ref.Ref) {
	for _, tuple := range cb.tuples {
		chunks = append(chunks, tuple.ref)
	}
	return
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
	cbr.currentReader = ReadValue(cbr.cursor.current().ref, cbr.cs).(blobLeaf).Reader()
	cbr.currentReader.Seek(int64(cbr.chunkOffset), 0)
}

// splitCompoundBlob chunks a compound list/blob into smaller compound
// lists/blobs. If no split was made the same compoundBlob is returned.
func splitCompoundBlob(cb compoundBlob, cs chunks.ChunkSink) compoundBlob {
	tuples := metaSequenceData{}

	startIndex := uint64(0)
	h := buzhash.NewBuzHash(objectWindowSize)

	for i := 0; i < len(cb.tuples); i++ {
		c := cb.tuples[i].ref
		digest := c.Digest()
		_, err := h.Write(digest[:])
		d.Chk.NoError(err)
		if h.Sum32()&objectPattern == objectPattern {
			h = buzhash.NewBuzHash(objectWindowSize)
			c := makeSubObject(cb, startIndex, uint64(i)+1, cs)
			startIndex = uint64(i) + 1
			tuples = append(tuples, metaTuple{c, cb.tuples[i].value})
		}
	}

	// No split, use original.
	if startIndex == 0 {
		return cb
	}

	// Add remaining.
	if startIndex != uint64(len(cb.tuples)) {
		c := makeSubObject(cb, startIndex, uint64(len(cb.tuples)), cs)
		tuples = append(tuples, metaTuple{c, cb.tuples[len(cb.tuples)-1].value})
	}

	// Single chunk, use original.
	if len(tuples) == 1 {
		return cb
	}

	// It is possible that the splitting the object produces the exact same
	// compound object.
	if len(tuples) == len(cb.tuples) {
		return cb
	}

	// Split again.
	return splitCompoundBlob(newCompoundBlob(tuples, cb.cs), cs)
}

func makeSubObject(cb compoundBlob, startIndex, endIndex uint64, cs chunks.ChunkSink) ref.Ref {
	d.Chk.True(endIndex-startIndex > 0)
	if endIndex-startIndex == 1 {
		return cb.tuples[startIndex].ref
	}

	tuples := make([]metaTuple, endIndex-startIndex)
	copy(tuples, cb.tuples[startIndex:endIndex])
	startOffset := uint64(0)
	if startIndex > 0 {
		startOffset = cb.tuples[startIndex-1].uint64Value()
	}
	for i := startIndex; i < endIndex; i++ {
		tuples[i-startIndex].value = UInt64(cb.tuples[i].uint64Value() - startOffset)
	}
	return WriteValue(newCompoundBlob(tuples, cb.cs), cs)
}
