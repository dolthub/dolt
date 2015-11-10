package types

import (
	"crypto/sha1"
	"errors"
	"io"
	"sort"

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

// Reader implements the Blob interface
func (cb compoundBlob) Reader() io.ReadSeeker {
	return &compoundBlobReader{cb: cb}
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
	cb               compoundBlob
	currentReader    io.ReadSeeker
	currentBlobIndex int
	offset           int64
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
	for cbr.currentBlobIndex < len(cbr.cb.tuples) {
		if cbr.currentReader == nil {
			if err = cbr.updateReader(); err != nil {
				return
			}
		}

		n, err = cbr.currentReader.Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF {
				err = nil
			}
			cbr.offset += int64(n)
			return
		}

		cbr.currentBlobIndex++
		cbr.currentReader = nil
	}
	return 0, io.EOF
}

func (cbr *compoundBlobReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(cbr.offset) + offset
	case 2:
		abs = int64(cbr.cb.Len()) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	cbr.offset = abs
	currentBlobIndex := cbr.currentBlobIndex
	cbr.currentBlobIndex = cbr.findBlobOffset(uint64(abs))
	if currentBlobIndex != cbr.currentBlobIndex {
		if err := cbr.updateReader(); err != nil {
			return int64(0), err
		}
	}
	if cbr.currentReader != nil {
		offset := abs
		if cbr.currentBlobIndex > 0 {
			offset -= int64(cbr.cb.tuples[cbr.currentBlobIndex-1].uint64Value())
		}
		if _, err := cbr.currentReader.Seek(offset, 0); err != nil {
			return 0, err
		}
	}

	return abs, nil
}

func (cbr *compoundBlobReader) findBlobOffset(abs uint64) int {
	return sort.Search(len(cbr.cb.tuples), func(i int) bool {
		return cbr.cb.tuples[i].uint64Value() > abs
	})
}

func (cbr *compoundBlobReader) updateReader() error {
	if cbr.currentBlobIndex < len(cbr.cb.tuples) {
		v := ReadValue(cbr.cb.tuples[cbr.currentBlobIndex].ref, cbr.cb.cs)
		cbr.currentReader = v.(Blob).Reader()
	} else {
		cbr.currentReader = nil
	}
	return nil
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
