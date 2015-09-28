// Package enc contains a very low-level JSON encoder/decoder. Serializes from interface{} to an io.Writer and deserializes from an io.Reader into an interface{}. Does not recursively process nested compound types; ref.Refs are treated like any other value.
// Supported types:
//   - bool
//   - int8
//   - int16
//   - int32
//   - int64
//   - uint8
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
//   - enc.CompoundList, a struct containing metadata for encoding a chunked list.
//   - TODO: Add support for structs, and make CompoundBlob/CompoundList use that. BUG #165
package enc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/attic-labs/noms/d"
)

// typedValue is used to tag an object so that Encode will encode it using the typed serialization format.
type typedValue interface {
	TypedValue() []interface{}
}

// Encode serializes v into dst, and panics on unsupported types.
func Encode(dst io.Writer, v interface{}) {
	d.Chk.NotNil(dst)
	switch v := v.(type) {
	case io.Reader:
		blobLeafEncode(dst, v)
	case typedValue:
		typedEncode(dst, v)
	default:
		jsonEncode(dst, v)
	}
}

// Decode deserializes data from r into an interface{}, and panics on unsupported encoded types.
func Decode(r io.Reader) interface{} {
	d.Chk.NotNil(r)

	// assumes all tags are same size, which they are for now.
	buffered := bufio.NewReaderSize(r, len(jsonTag))
	prefix, err := buffered.Peek(len(jsonTag))
	d.Exp.NoError(err)

	if bytes.Equal(prefix, jsonTag) {
		return jsonDecode(buffered)
	} else if bytes.Equal(prefix, blobTag) {
		return blobLeafDecode(buffered)
	} else if bytes.Equal(prefix, typedTag) {
		return typedDecode(buffered)
	}

	d.Exp.Fail(fmt.Sprintf("Unsupported chunk tag: %+v", prefix))
	return nil
}
