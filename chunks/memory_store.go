package chunks

import (
	"bytes"
	"crypto/sha1"
	"flag"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

// An in-memory implementation of store.ChunkStore. Useful mainly for tests.
type MemoryStore struct {
	data map[ref.Ref][]byte
	memoryRootTracker
}

func (ms *MemoryStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	if b, ok := ms.data[ref]; ok {
		return ioutil.NopCloser(bytes.NewReader(b)), nil
	}
	return nil, nil
}

func (ms *MemoryStore) Put() ChunkWriter {
	return &memoryChunkWriter{ms, &bytes.Buffer{}, ref.Ref{}}
}

func (ms *MemoryStore) Len() int {
	return len(ms.data)
}

type memoryChunkWriter struct {
	ms  *MemoryStore
	buf *bytes.Buffer
	ref ref.Ref
}

func (w *memoryChunkWriter) Write(data []byte) (int, error) {
	return w.buf.Write(data)
}

func (w *memoryChunkWriter) Ref() (ref.Ref, error) {
	dbg.Chk.NoError(w.Close())
	return w.ref, nil
}

func (w *memoryChunkWriter) Close() error {
	if w.buf == nil {
		return nil
	}

	r := ref.New(sha1.Sum(w.buf.Bytes()))
	if w.ms.data == nil {
		w.ms.data = map[ref.Ref][]byte{}
	}
	w.ms.data[r] = w.buf.Bytes()

	w.buf = nil
	w.ms = nil
	w.ref = r
	return nil
}

type memoryStoreFlags struct {
	use *bool
}

func memoryFlags() memoryStoreFlags {
	return memoryStoreFlags{
		flag.Bool("memory-store", false, "use a memory-based (ephemeral, and private to this application) chunkstore"),
	}
}

func (f memoryStoreFlags) createStore() ChunkStore {
	if *f.use {
		return &MemoryStore{}
	} else {
		return nil
	}
}
