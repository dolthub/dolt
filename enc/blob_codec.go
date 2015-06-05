package enc

import (
	"bytes"
	"io"

	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

var (
	blobTag = []byte("b ")
)

func encodeBlob(b types.Blob, s store.ChunkSink) (r ref.Ref, err error) {
	return encodeBlobImpl(blobTag, b, s)
}

func encodeBlobImpl(tag []byte, b types.Blob, s store.ChunkSink) (r ref.Ref, err error) {
	w := s.Put()
	if _, err = w.Write(tag); err != nil {
		return
	}
	if _, err = io.Copy(w, b.Read()); err != nil {
		return
	}
	return w.Ref()
}

func decodeBlob(r io.Reader, s store.ChunkSource) (types.Blob, error) {
	b, err := decodeBlobImpl(blobTag, r, s)
	if err != nil {
		return nil, err
	}
	return types.NewBlob(b), nil
}

func decodeBlobImpl(tag []byte, r io.Reader, s store.ChunkSource) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := io.CopyN(buf, r, int64(len(tag)))
	if err != nil {
		return nil, err
	}
	Chk.True(bytes.Equal(buf.Bytes(), tag))

	buf.Truncate(0)
	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
