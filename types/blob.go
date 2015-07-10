package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/ref"
)

type Blob struct {
	data []byte
	cr   *cachedRef
}

func (fb Blob) Reader() io.Reader {
	return bytes.NewBuffer(fb.data)
}

func (fb Blob) Len() uint64 {
	return uint64(len(fb.data))
}

func (fb Blob) Ref() ref.Ref {
	return fb.cr.Ref(fb)
}

func (fb Blob) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fb.Ref() == other.Ref()
	}
}

func NewBlob(data []byte) Blob {
	return Blob{data, &cachedRef{}}
}
