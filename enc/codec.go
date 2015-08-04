package enc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/attic-labs/noms/dbg"
)

// Encode serializes v into w, and panics on unsupported types.
// Supported types include:
//   - Primitives
//   - Strings
//   - ref.Ref
//   - io.Reader, for blobs
//   - enc.List of any encodeable type
//   - enc.Map of any encodeable type -> any encodeable type
//   - enc.Set of any encodeable type
//   - enc.CompoundBlob, a struct containing metadata for encoding a chunked blob.
//   - TODO: Add support for structs, and make CompoundBlob use that. BUG #165
func Encode(v interface{}, w io.Writer) error {
	dbg.Chk.NotNil(w)
	switch v := v.(type) {
	case io.Reader:
		return blobLeafEncode(v, w)
	default:
		return jsonEncode(v, w)
	}
}

// Decode deserializes data from r into an interface{}, and panics on unsupported encoded types.
// Supported types include:
//   - Primitives
//   - Strings
//   - ref.Ref
//   - io.Reader, for blobs
//   - enc.List of any encodeable type
//   - enc.Map of any encodeable type -> any encodeable type
//   - enc.Set of any encodeable type
//   - enc.CompoundBlob, a struct containing metadata for encoding a chunked blob.
//   - TODO: Add support for structs, and make CompoundBlob use that. BUG #165
func Decode(r io.Reader) (interface{}, error) {
	dbg.Chk.NotNil(r)

	// assumes all tags are same size, which they are for now.
	buffered := bufio.NewReaderSize(r, len(jsonTag))
	prefix, err := buffered.Peek(len(jsonTag))
	if err != nil {
		return nil, err
	}

	if bytes.Equal(prefix, jsonTag) {
		return jsonDecode(buffered)
	} else if bytes.Equal(prefix, blobTag) {
		return blobLeafDecode(buffered)
	}
	return nil, fmt.Errorf("Unsupported chunk tag: %+v", prefix)
}
