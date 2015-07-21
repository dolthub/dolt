package chunks

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

// ReadThroughStore is a store that consists of two other stores. A caching and
// a backing store. All reads check the caching store first and if the ref is
// present there the caching store is used. If not present the backing store is
// used and the value gets cached in the caching store. All writes go directly
// to the backing store.
type ReadThroughStore struct {
	cachingStore ChunkStore
	backingStore ChunkStore
}

func NewReadThroughStore(cachingStore ChunkStore, backingStore ChunkStore) ReadThroughStore {
	return ReadThroughStore{cachingStore, backingStore}
}

// forwardCloser closes multiple io.Closer objects.
type forwardCloser struct {
	io.Reader
	cs []io.Closer
}

func (fc forwardCloser) Close() error {
	for _, c := range fc.cs {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (rts ReadThroughStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	r, err := rts.cachingStore.Get(ref)
	if r != nil && err == nil {
		return r, err
	}
	r, err = rts.backingStore.Get(ref)
	if r == nil || err != nil {
		return r, err
	}

	w := rts.cachingStore.Put()
	tr := io.TeeReader(r, w)
	return forwardCloser{tr, []io.Closer{r, w}}, nil
}

type readThroughChunkWriter struct {
	cws []ChunkWriter
}

func (w readThroughChunkWriter) Ref() (r ref.Ref, err error) {
	for _, cw := range w.cws {
		if r, err = cw.Ref(); err != nil {
			return
		}
	}
	return
}

func (w readThroughChunkWriter) Write(p []byte) (n int, err error) {
	for _, cw := range w.cws {
		if n, err = cw.Write(p); err != nil {
			return
		}
	}
	return
}

func (w readThroughChunkWriter) Close() (err error) {
	for _, cw := range w.cws {
		if err = cw.Close(); err != nil {
			return
		}
	}
	return
}

func (rts ReadThroughStore) Put() ChunkWriter {
	bw := rts.backingStore.Put()
	cw := rts.cachingStore.Put()
	return readThroughChunkWriter{[]ChunkWriter{bw, cw}}
}

func (rts ReadThroughStore) Root() ref.Ref {
	return rts.backingStore.Root()
}

func (rts ReadThroughStore) UpdateRoot(current, last ref.Ref) bool {
	return rts.backingStore.UpdateRoot(current, last)
}
