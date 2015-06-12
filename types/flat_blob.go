package types

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/ref"
)

type flatBlob struct {
	data []byte
	cr   *cachedRef
}

func (fb flatBlob) Reader() io.Reader {
	return bytes.NewBuffer(fb.data)
}

func (fb flatBlob) Len() uint64 {
	return uint64(len(fb.data))
}

func (fb flatBlob) Ref() ref.Ref {
	return fb.cr.Ref(fb)
}

func (fb flatBlob) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fb.Ref() == other.Ref()
	}
}
