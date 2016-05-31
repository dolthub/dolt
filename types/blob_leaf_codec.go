// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

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
