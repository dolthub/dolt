package types

import (
	"bytes"
	"io"
	"io/ioutil"

	. "github.com/attic-labs/noms/dbg"
)

type flatBlob struct {
	data []byte
}

func (fb flatBlob) Reader() io.Reader {
	return bytes.NewBuffer(fb.data)
}

func (fb flatBlob) Len() uint64 {
	return uint64(len(fb.data))
}

func (fb flatBlob) Equals(other Value) bool {
	// TODO: See note about content addressing in flat_list.go.
	if other, ok := other.(Blob); ok {
		otherData, err := ioutil.ReadAll(other.Reader())
		Chk.NoError(err)
		return bytes.Equal(fb.data, otherData)
	}
	return false
}
