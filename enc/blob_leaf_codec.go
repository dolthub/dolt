package enc

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/d"
)

var (
	blobTag = []byte("b ")
)

func blobLeafEncode(dst io.Writer, src io.Reader) {
	_, err := dst.Write(blobTag)
	d.Exp.NoError(err)
	_, err = io.Copy(dst, src)
	d.Exp.NoError(err)
}

func blobLeafDecode(src io.Reader) io.Reader {
	buf := &bytes.Buffer{}
	_, err := io.CopyN(buf, src, int64(len(blobTag)))
	d.Exp.NoError(err)
	d.Exp.True(bytes.Equal(buf.Bytes(), blobTag), "Cannot blobLeafDecode - invalid prefix")
	return src
}
