package chunks

import (
	"sync"

	"github.com/attic-labs/noms/d"
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

func (ms *MemoryStore) PutMany(chunks []Chunk) (e BackpressureError) {
	for _, c := range chunks {
		ms.Put(c)
	}
	return
}

func (ms *MemoryStore) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.data)
}

func (ms *MemoryStore) Close() error {
	return nil
}

func NewMemoryStoreFactory() Factory {
	return &MemoryStoreFactory{map[string]*MemoryStore{}}
}

type MemoryStoreFactory struct {
	stores map[string]*MemoryStore
}

func (f *MemoryStoreFactory) CreateStore(ns string) ChunkStore {
	d.Chk.NotNil(f.stores, "Cannot use LevelDBStoreFactory after Shutter().")
	if cs, present := f.stores[ns]; present {
		return cs
	}
	f.stores[ns] = NewMemoryStore()
	return f.stores[ns]
}

func (f *MemoryStoreFactory) Shutter() {
	f.stores = map[string]*MemoryStore{}
}
