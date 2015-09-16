package chunks

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ReadThroughStore is a store that consists of two other stores. A caching and
// a backing store. All reads check the caching store first and if the ref is
// present there the caching store is used. If not present the backing store is
// used and the value gets cached in the caching store. All writes go directly
// to the backing store.
type ReadThroughStore struct {
	io.Closer
	cachingStore ChunkStore
	backingStore ChunkStore
	putCount     int
}

func NewReadThroughStore(cachingStore ChunkStore, backingStore ChunkStore) ReadThroughStore {
	return ReadThroughStore{ioutil.NopCloser(nil), cachingStore, backingStore, 0}
}

func (rts ReadThroughStore) Get(ref ref.Ref) []byte {
	data := rts.cachingStore.Get(ref)
	if data != nil {
		return data
	}
	data = rts.backingStore.Get(ref)
	if data == nil {
		return data
	}

	w := rts.cachingStore.Put()
	_, err := io.Copy(w, bytes.NewReader(data))
	d.Chk.NoError(err)
	w.Close()

	return data
}

type readThroughChunkWriter struct {
	cws []ChunkWriter
}

func (w readThroughChunkWriter) Ref() (r ref.Ref) {
	for _, cw := range w.cws {
		r = cw.Ref()
	}
	return
}

func (w readThroughChunkWriter) Write(p []byte) (n int, err error) {
	for _, cw := range w.cws {
		n, err = cw.Write(p)
		d.Chk.NoError(err)
	}
	return
}

func (w readThroughChunkWriter) Close() (err error) {
	for _, cw := range w.cws {
		cw.Close()
	}
	return
}

func (rts ReadThroughStore) Has(ref ref.Ref) bool {
	return rts.cachingStore.Has(ref) || rts.backingStore.Has(ref)
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
