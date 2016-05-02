package types

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ValueReader is an interface that knows how to read Noms Values, e.g. datas/DataStore. Required to avoid import cycle between this package and the package that implements Value reading.
type ValueReader interface {
	ReadValue(r ref.Ref) Value
}

// ValueReadWriter is an interface that knows how to read and write Noms Values, e.g. datas/DataStore. Required to avoid import cycle between this package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
}

// DecodeChunk decodes a value from a chunk source. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func DecodeChunk(c chunks.Chunk, vr ValueReader) Value {
	if c.IsEmpty() {
		return nil
	}

	v := decode(bytes.NewReader(c.Data()))

	switch v := v.(type) {
	case io.Reader:
		data, err := ioutil.ReadAll(v)
		d.Chk.NoError(err)
		return newBlobLeaf(data)
	case []interface{}:
		return fromTypedEncodeable(v, vr)
	}
	panic("Unreachable")
}
