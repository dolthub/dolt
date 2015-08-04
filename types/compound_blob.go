package types

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	length       uint64
	childLengths []uint64
	blobs        []Future
	ref          *ref.Ref
	cs           chunks.ChunkSource
}

// Reader implements the Blob interface
func (cb compoundBlob) Reader() io.Reader {
	return &compoundBlobReader{cb.blobs, nil, cb.cs}
}

type compoundBlobReader struct {
	blobs []Future
	r     io.Reader
	cs    chunks.ChunkSource
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
	for len(cbr.blobs) > 0 {
		if cbr.r == nil {
			var v Value
			v, err = cbr.blobs[0].Deref(cbr.cs)
			if err != nil {
				return
			}
			cbr.r = v.(Blob).Reader()
		}
		n, err = cbr.r.Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF {
				err = nil
			}
			return
		}

		cbr.blobs = cbr.blobs[1:]
		cbr.r = nil
	}
	return 0, io.EOF
}

// Len implements the Blob interface
func (cb compoundBlob) Len() uint64 {
	return cb.length
}

func (cb compoundBlob) Ref() ref.Ref {
	return ensureRef(cb.ref, cb)
}

func (cb compoundBlob) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return cb.Ref() == other.Ref()
	}
}

func (cb compoundBlob) Chunks() (futures []Future) {
	for _, f := range cb.blobs {
		if f, ok := f.(*unresolvedFuture); ok {
			futures = append(futures, f)
		}
	}
	return
}
