package store

import (
	"hash"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/ref"
)

type NopSink struct {
}

func (NopSink) Put() ChunkWriter {
	// Sigh... Go is so dreamy.
	return NopWriter{ref.NewHash(), ioutil.NopCloser(nil)}
}

type NopWriter struct {
	hash.Hash
	io.Closer
}

func (nw NopWriter) Ref() (ref.Ref, error) {
	return ref.FromHash(nw.Hash), nil
}
