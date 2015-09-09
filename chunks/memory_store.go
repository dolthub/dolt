package chunks

import (
	"bytes"
	"flag"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/ref"
)

// An in-memory implementation of store.ChunkStore. Useful mainly for tests.
type MemoryStore struct {
	data map[ref.Ref][]byte
	memoryRootTracker
}

func (ms *MemoryStore) Get(ref ref.Ref) io.ReadCloser {
	if b, ok := ms.data[ref]; ok {
		return ioutil.NopCloser(bytes.NewReader(b))
	}
	return nil
}

func (ms *MemoryStore) Has(r ref.Ref) bool {
	if ms.data == nil {
		return false
	}
	_, ok := ms.data[r]
	return ok
}

func (ms *MemoryStore) Put() ChunkWriter {
	return newChunkWriter(ms.write)
}

func (ms *MemoryStore) write(r ref.Ref, buff *bytes.Buffer) {
	if ms.Has(r) {
		return
	}

	if ms.data == nil {
		ms.data = map[ref.Ref][]byte{}
	}
	ms.data[r] = buff.Bytes()
}

func (ms *MemoryStore) Len() int {
	return len(ms.data)
}

func (l *MemoryStore) Close() error {
	return nil
}

type memoryStoreFlags struct {
	use *bool
}

func memoryFlags(prefix string) memoryStoreFlags {
	return memoryStoreFlags{
		flag.Bool(prefix+"mem", false, "use a memory-based (ephemeral, and private to this application) chunkstore"),
	}
}

func (f memoryStoreFlags) createStore() ChunkStore {
	if *f.use {
		return &MemoryStore{}
	} else {
		return nil
	}
}
