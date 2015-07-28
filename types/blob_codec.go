package types

import (
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

var (
	blobTag = []byte("b ")
)

func blobEncode(b Blob, s chunks.ChunkSink) (r ref.Ref, err error) {
	w := s.Put()
	if _, err = w.Write(blobTag); err != nil {
		return
	}
	if _, err = io.Copy(w, b.Reader()); err != nil {
		return
	}
	return w.Ref()
}

func blobDecode(r io.Reader, s chunks.ChunkSource) (Value, error) {
	// Skip the blobTag
	_, err := ioutil.ReadAll(io.LimitReader(r, int64(len(blobTag))))
	if err != nil {
		return nil, err
	}
	return NewBlob(r), nil
}
