package enc

import (
	"io"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

var (
	stringTag = []byte("s ")
)

func encodeString(b types.String, s store.ChunkSink) (r ref.Ref, err error) {
	return encodeBlobImpl(stringTag, b, s)
}

func decodeString(r io.Reader, s store.ChunkSource) (types.Value, error) {
	b, err := decodeBlobImpl(stringTag, r, s)
	if err != nil {
		return nil, err
	}
	return types.StringFromBytes(b), nil
}
