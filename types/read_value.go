package types

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

// Reads and decodes a value from a chunk source.
func ReadValue(ref ref.Ref, cs chunks.ChunkSource) (Value, error) {
	Chk.NotNil(cs)
	reader, err := cs.Get(ref)
	if reader == nil {
		return nil, errors.New("Chunk not present")
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

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

func MustReadValue(ref ref.Ref, cs chunks.ChunkSource) Value {
	val, err := ReadValue(ref, cs)
	Chk.NoError(err)
	return val
}
