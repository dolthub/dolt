package enc

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

// Reads and decodes a value from a chunk source.
func ReadValue(ref ref.Ref, cs store.ChunkSource) (types.Value, error) {
	reader, err := cs.Get(ref)
	if err != nil {
		return nil, err
	}

	// assumes all tags are same size, which they are for now.
	buffered := bufio.NewReaderSize(reader, len(jsonTag))
	prefix, err := buffered.Peek(len(jsonTag))
	if err != nil {
		return nil, err
	}

	if bytes.Equal(prefix, jsonTag) {
		return jsonDecode(buffered, cs)
	}

	if bytes.Equal(prefix, blobTag) {
		return blobDecode(buffered, cs)
	}

	return nil, fmt.Errorf("Unsupported chunk tag: %+v", prefix)
}
