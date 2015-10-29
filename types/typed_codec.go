package types

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/attic-labs/noms/d"
)

var (
	typedTag = []byte("t ")
)

func appendCompactedJSON(dst io.Writer, v interface{}) {
	buff := &bytes.Buffer{}
	err := json.NewEncoder(buff).Encode(v)
	d.Exp.NoError(err)
	buff2 := &bytes.Buffer{}
	err = json.Compact(buff2, buff.Bytes())
	d.Chk.NoError(err)
	_, err = io.Copy(dst, buff2)
	d.Chk.NoError(err)
}

func typedEncode(dst io.Writer, v interface{}) {
	_, err := dst.Write(typedTag)
	d.Exp.NoError(err)
	appendCompactedJSON(dst, v)
	return
}

func typedDecode(reader io.Reader) []interface{} {
	prefix := make([]byte, len(typedTag))
	_, err := io.ReadFull(reader, prefix)
	d.Exp.NoError(err)

	// Since typedDecode is private, and Decode() should have checked this, it is invariant that the prefix will match.
	d.Chk.EqualValues(typedTag[:], prefix, "Cannot typedDecode - invalid prefix")

	var v []interface{}
	err = json.NewDecoder(reader).Decode(&v)
	d.Exp.NoError(err)

	return v
}
