package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/ref"
)

type Blob struct {
	data []byte
	ref  *ref.Ref
}

func (fb Blob) Reader() io.Reader {
	return bytes.NewBuffer(fb.data)
}

func (fb Blob) Len() uint64 {
	return uint64(len(fb.data))
}

func (fb Blob) Ref() ref.Ref {
	return ensureRef(fb.ref, fb)
}

func (fb Blob) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fb.Ref() == other.Ref()
	}
}

func (fb Blob) Chunks() []Future {
	return nil
}

func NewBlob(data []byte) Blob {
	return Blob{data, &ref.Ref{}}
}

func BlobFromVal(v Value) Blob {
	return v.(Blob)
}
