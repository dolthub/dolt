package types

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/attic-labs/noms/d"
)

// Encode serializes v into dst, and panics on unsupported types.
func encode(dst io.Writer, v interface{}) {
	d.Chk.NotNil(dst)
	switch v := v.(type) {
	case io.Reader:
		blobLeafEncode(dst, v)
	default:
		typedEncode(dst, v)
	}
}

// Decode deserializes data from r into an interface{}, and panics on unsupported encoded types.
func decode(r io.Reader) interface{} {
	d.Chk.NotNil(r)

	// assumes all tags are same size, which they are for now.
	buffered := bufio.NewReaderSize(r, len(typedTag))
	prefix, err := buffered.Peek(len(typedTag))
	d.Exp.NoError(err)

	if bytes.Equal(prefix, blobTag) {
		return blobLeafDecode(buffered)
	} else if bytes.Equal(prefix, typedTag) {
		return typedDecode(buffered)
	}

	d.Exp.Fail(fmt.Sprintf("Unsupported chunk tag: %+v", prefix))
	return nil
}
