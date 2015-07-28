package types

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/dbg"
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
	readers := make([]io.Reader, len(cb.blobs))
	for i, b := range cb.blobs {
		// BUG 155 - Should provide Seek and Write... Maybe even have Blob implement ReadWriteSeeker
		v, err := b.Deref(cb.cs)
		// TODO: This is ugly. See comment in list.go@Get.
		dbg.Chk.NoError(err)
		readers[i] = v.(blobLeaf).Reader()
	}
	return io.MultiReader(readers...)
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
