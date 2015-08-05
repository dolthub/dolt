package enc

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/dbg"
)

var (
	blobTag = []byte("b ")
)

func blobLeafEncode(dst io.Writer, src io.Reader) (err error) {
	if _, err = dst.Write(blobTag); err != nil {
		return
	}
	if _, err = io.Copy(dst, src); err != nil {
		return
	}
	return
}

func blobLeafDecode(src io.Reader) (io.Reader, error) {
	buf := &bytes.Buffer{}
	_, err := io.CopyN(buf, src, int64(len(blobTag)))
	if err != nil {
		return nil, err
	}
	dbg.Chk.True(bytes.Equal(buf.Bytes(), blobTag), "Cannot blobLeafDecode - invalid prefix")

	return src, nil
}
