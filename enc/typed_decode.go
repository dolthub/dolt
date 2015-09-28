package enc

import (
	"encoding/json"
	"io"

	"github.com/attic-labs/noms/d"
)

type typedValueWrapper struct {
	v []interface{}
}

func (t typedValueWrapper) TypedValue() []interface{} {
	return t.v
}

func typedDecode(reader io.Reader) typedValue {
	prefix := make([]byte, len(typedTag))
	_, err := io.ReadFull(reader, prefix)
	d.Exp.NoError(err)

	// Since typedDecode is private, and Decode() should have checked this, it is invariant that the prefix will match.
	d.Chk.EqualValues(typedTag[:], prefix, "Cannot typedDecode - invalid prefix")

	var v []interface{}
	err = json.NewDecoder(reader).Decode(&v)
	d.Exp.NoError(err)

	return typedValueWrapper{v}
}
