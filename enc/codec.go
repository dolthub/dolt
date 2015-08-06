// Package enc contains a very low-level JSON encoder/decoder. Serializes from interface{} to an io.Writer and deserializes from an io.Reader into an interface{}. Does not recursively process nested compound types; ref.Refs are treated like any other value.
// Supported types:
//   - bool
//   - int16
//   - int32
//   - int64
//   - uint16
//   - uint32
//   - uint64
//   - float32
//   - float64
//   - string
//   - ref.Ref
//   - io.Reader, for blobs
//   - enc.List of any encodeable type
//   - enc.Map of any encodeable type -> any encodeable type
//   - enc.Set of any encodeable type
//   - enc.CompoundBlob, a struct containing metadata for encoding a chunked blob.
//   - TODO: Add support for structs, and make CompoundBlob use that. BUG #165
package enc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/attic-labs/noms/dbg"
)

// Encode serializes v into dst, and panics on unsupported types.
func Encode(dst io.Writer, v interface{}) error {
	dbg.Chk.NotNil(dst)
	switch v := v.(type) {
	case io.Reader:
		return blobLeafEncode(dst, v)
	default:
		return jsonEncode(dst, v)
	}
}

// Decode deserializes data from r into an interface{}, and panics on unsupported encoded types.
func Decode(r io.Reader) (interface{}, error) {
	dbg.Chk.NotNil(r)

	// assumes all tags are same size, which they are for now.
	buffered := bufio.NewReaderSize(r, len(jsonTag))
	prefix, err := buffered.Peek(len(jsonTag))
	// Consider rejiggering this error handling with BUG #176.
	if err != nil {
		return nil, err
	}

	if bytes.Equal(prefix, jsonTag) {
		return jsonDecode(buffered)
	} else if bytes.Equal(prefix, blobTag) {
		return blobLeafDecode(buffered)
	}
	// Consider rejiggering this error handling with BUG #176.
	return nil, fmt.Errorf("Unsupported chunk tag: %+v", prefix)
}
