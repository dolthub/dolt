package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/ref"
)

// blobLeaf represents a leaf in a compoundBlob.
// It implements the Blob interface.
type blobLeaf struct {
	data []byte
	ref  *ref.Ref
}

func newBlobLeaf(data []byte) blobLeaf {
	return blobLeaf{data, &ref.Ref{}}
}

// Reader implements the Blob interface
func (bl blobLeaf) Reader() io.ReadSeeker {
	return bytes.NewReader(bl.data)
}

// Len implements the Blob interface
func (bl blobLeaf) Len() uint64 {
	return uint64(len(bl.data))
}

func (bl blobLeaf) Ref() ref.Ref {
	return EnsureRef(bl.ref, bl)
}

func (bl blobLeaf) Chunks() []RefBase {
	return nil
}

func (bl blobLeaf) ChildValues() []Value {
	return nil
}

func (bl blobLeaf) Type() Type {
	return typeForBlob
}

func (bl blobLeaf) Equals(other Value) bool {
	return other != nil && typeForBlob.Equals(other.Type()) && bl.Ref() == other.Ref()
}
