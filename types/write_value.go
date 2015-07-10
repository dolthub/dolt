package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// TODO: This ends up loading the entire value recusrively. We need to change the encoder to look at the futures directly and not expand them.
func WriteValue(v Value, cs chunks.ChunkSink) (ref.Ref, error) {
	switch v := v.(type) {
	case Blob:
		return blobEncode(v, cs)
	default:
		return jsonEncode(v, cs)
	}
}
