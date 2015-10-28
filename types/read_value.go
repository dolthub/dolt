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
		tv := fromTypedEncodeable(v, cs)
		if tv, ok := tv.(compoundBlobStruct); ok {
			return convertToCompoundBlob(tv, cs)
		}
		return tv
	}
	panic("Unreachable")
}

func convertToCompoundBlob(cbs compoundBlobStruct, cs chunks.ChunkSource) compoundBlob {
	offsets := cbs.Offsets().Def()
	refs := cbs.Blobs().Def()
	futures := make([]Future, len(refs))
	for i, r := range refs {
		futures[i] = futureFromRef(r)
	}
	return newCompoundBlob(offsets, futures, cs)
}
