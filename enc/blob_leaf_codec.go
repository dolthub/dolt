package enc

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/dbg"
)

var (
	blobTag = []byte("b ")
)

func blobLeafEncode(b io.Reader, w io.Writer) (err error) {
	if _, err = w.Write(blobTag); err != nil {
		return
	}
	if _, err = io.Copy(w, b); err != nil {
		return
	}
	return
}

func blobLeafDecode(r io.Reader) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := io.CopyN(buf, r, int64(len(blobTag)))
	if err != nil {
		return nil, err
	}
	dbg.Chk.True(bytes.Equal(buf.Bytes(), blobTag))

	buf.Truncate(0)
	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
