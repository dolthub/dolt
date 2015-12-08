package chunks

import (
	"flag"
	"sync"

	"github.com/attic-labs/noms/ref"
)

// An in-memory implementation of store.ChunkStore. Useful mainly for tests.
type MemoryStore struct {
	data map[ref.Ref]Chunk
	memoryRootTracker
	mu *sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu: &sync.Mutex{},
	}
}

func (ms *MemoryStore) Get(ref ref.Ref) Chunk {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if c, ok := ms.data[ref]; ok {
		return c
	}
	return EmptyChunk
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

func (ms *MemoryStore) Put(c Chunk) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.data == nil {
		ms.data = map[ref.Ref]Chunk{}
	}
	ms.data[c.Ref()] = c
}

func (ms *MemoryStore) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.data)
}

func (l *MemoryStore) Close() error {
	return nil
}

type MemoryStoreFlags struct {
	use *bool
}

func MemoryFlags(prefix string) MemoryStoreFlags {
	return MemoryStoreFlags{
		flag.Bool(prefix+"mem", false, "use a memory-based (ephemeral, and private to this application) chunkstore"),
	}
}

func (f MemoryStoreFlags) CreateStore() ChunkStore {
	if *f.use {
		return NewMemoryStore()
	} else {
		return nil
	}
}
