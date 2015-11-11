package types

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ReadValue reads and decodes a value from a chunk source. It is not considered an error for the requested chunk to be absent from cs; in this case, the function simply returns nil, nil.
func ReadValue(r ref.Ref, cs chunks.ChunkSource) Value {
	d.Chk.NotNil(cs)
	c := cs.Get(r)
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
		return fromTypedEncodeable(v, cs)
	}
	panic("Unreachable")
}
