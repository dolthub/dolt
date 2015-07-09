package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func WriteValue(v Value, cs chunks.ChunkSink) (ref.Ref, error) {
	switch v := v.(type) {
	case Blob:
		return blobEncode(v, cs)
	default:
		return jsonEncode(v, cs)
	}
}
