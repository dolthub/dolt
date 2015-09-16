package chunks

import (
	"flag"
	"sync"

	"github.com/attic-labs/noms/ref"
)

// An in-memory implementation of store.ChunkStore. Useful mainly for tests.
type MemoryStore struct {
	data map[ref.Ref][]byte
	memoryRootTracker
	mu *sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu: &sync.Mutex{},
	}
}

func (ms *MemoryStore) Get(ref ref.Ref) []byte {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if b, ok := ms.data[ref]; ok {
		return b
	}
	return nil
}

func (ms *MemoryStore) Has(r ref.Ref) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.data == nil {
		return false
	}
	_, ok := ms.data[r]
	return ok
}

func (ms *MemoryStore) Put() ChunkWriter {
	return NewChunkWriter(ms.write)
}

func (ms *MemoryStore) write(r ref.Ref, data []byte) {
	if ms.Has(r) {
		return
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.data == nil {
		ms.data = map[ref.Ref][]byte{}
	}
	ms.data[r] = data
}

func (ms *MemoryStore) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
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
		return NewMemoryStore()
	} else {
		return nil
	}
}
