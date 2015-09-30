package types

import (
	"errors"
	"io"
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	compoundObject
}

func newCompoundBlob(offsets []uint64, futures []Future, cs chunks.ChunkSource) compoundBlob {
	return compoundBlob{compoundObject{offsets, futures, &ref.Ref{}, cs}}
}

// Reader implements the Blob interface
func (cb compoundBlob) Reader() io.ReadSeeker {
	return &compoundBlobReader{cb: cb}
}

type compoundBlobReader struct {
	cb               compoundBlob
	currentReader    io.ReadSeeker
	currentBlobIndex int
	offset           int64
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
	for cbr.currentBlobIndex < len(cbr.cb.futures) {
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
			offset -= int64(cbr.cb.offsets[cbr.currentBlobIndex-1])
		}
		if _, err := cbr.currentReader.Seek(offset, 0); err != nil {
			return 0, err
		}
	}

	return abs, nil
}

func (cbr *compoundBlobReader) findBlobOffset(abs uint64) int {
	return sort.Search(len(cbr.cb.offsets), func(i int) bool {
		return cbr.cb.offsets[i] > abs
	})
}

func (cbr *compoundBlobReader) updateReader() error {
	if cbr.currentBlobIndex < len(cbr.cb.futures) {
		v := cbr.cb.futures[cbr.currentBlobIndex].Deref(cbr.cb.cs)
		cbr.currentReader = v.(Blob).Reader()
	} else {
		cbr.currentReader = nil
	}
	return nil
}

func (cb compoundBlob) Ref() ref.Ref {
	return ensureRef(cb.ref, cb)
}

func (cb compoundBlob) Equals(other Value) bool {
	if other, ok := other.(compoundBlob); ok {
		return cb.Ref() == other.Ref()
	}
	return false
}

func (cb compoundBlob) TypeRef() TypeRef {
	return typeRefForBlob
}
