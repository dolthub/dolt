// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
)

// ValueReader is an interface that knows how to read Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value reading.
type ValueReader interface {
	ReadValue(h hash.Hash) Value
}

// ValueReadWriter is an interface that knows how to read and write Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
}

// DecodeChunk decodes a value from a chunk source. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func DecodeChunk(c chunks.Chunk, vr ValueReader) (v Value) {
	if c.IsEmpty() {
		return nil
	}

	decoded := decode(bytes.NewReader(c.Data()))
	switch decoded := decoded.(type) {
	case io.Reader:
		data, err := ioutil.ReadAll(decoded)
		d.Chk.NoError(err)
		v = newBlob(newBlobLeafSequence(vr, data))
	case []interface{}:
		v = fromTypedEncodeable(decoded, vr)
	default:
		panic("Unreachable")
	}
	if cacher, ok := v.(hashCacher); ok {
		assignHash(cacher, c.Hash())
	}
	return
}
