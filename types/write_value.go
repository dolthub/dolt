package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func WriteValue(in Value, cs chunks.ChunkSink) (r ref.Ref, out Value, err error) {
	switch in := in.(type) {
	case Blob:
		r, err = blobEncode(in, cs)
	default:
		r, err = jsonEncode(in, cs)
	}
	out = in
	out.Release()
	return
}
