package enc

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
