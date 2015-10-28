package types

import (
	"encoding/json"
	"io"

	"github.com/attic-labs/noms/d"
)

var (
	typedTag = []byte("t ")
)

func typedEncode(dst io.Writer, v interface{}) {
	_, err := dst.Write(typedTag)
	d.Exp.NoError(err)
	err = json.NewEncoder(dst).Encode(v)
	d.Exp.NoError(err)
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
