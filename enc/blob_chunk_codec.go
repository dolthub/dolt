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
	blobChunkTag = []byte("b ")
)

func encodeBlobChunk(b types.Blob, s store.ChunkSink) (r ref.Ref, err error) {
	w := s.Put()
	if _, err = w.Write(blobChunkTag); err != nil {
		return
	}
	if _, err = io.Copy(w, b.Read()); err != nil {
		return
	}
	return w.Ref()
}

func decodeBlobChunk(r io.Reader, s store.ChunkSource) (types.Value, error) {
	buf := &bytes.Buffer{}
	_, err := io.CopyN(buf, r, int64(len(blobChunkTag)))
	if err != nil {
		return nil, err
	}
	Chk.True(bytes.Equal(buf.Bytes(), blobChunkTag))

	buf.Truncate(0)
	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}
	return types.NewBlob(buf.Bytes()), nil
}
