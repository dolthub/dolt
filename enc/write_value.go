package enc

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func WriteValue(v types.Value, cs chunks.ChunkSink) (ref.Ref, error) {
	switch v := v.(type) {
	case types.Blob:
		return blobEncode(v, cs)
	default:
		return jsonEncode(v, cs)
	}
}
