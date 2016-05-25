package chunks

import (
	"sync"

	"github.com/attic-labs/noms/hash"
	"github.com/attic-labs/testify/assert"
)

func assertInputInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(h)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func assertInputNotInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	data := s.Get(h)
	assert.Nil(data, "Shouldn't have gotten data for %s", h.String())
}

type TestStore struct {
	MemoryStore
	Reads  int
	Hases  int
	Writes int
}

func NewTestStore() *TestStore {
	return &TestStore{
		MemoryStore: MemoryStore{
			mu: &sync.Mutex{},
		},
	}
}

func (s *TestStore) Get(h hash.Hash) Chunk {
	s.Reads++
	return s.MemoryStore.Get(h)
}

func (s *TestStore) Has(h hash.Hash) bool {
	s.Hases++
	return s.MemoryStore.Has(h)
}

func (s *TestStore) Put(c Chunk) {
	s.Writes++
	s.MemoryStore.Put(c)
}

func (s *TestStore) PutMany(chunks []Chunk) (e BackpressureError) {
	for _, c := range chunks {
		s.Put(c)
	}
	return
}

type testStoreFactory struct {
	stores map[string]*TestStore
}

func NewTestStoreFactory() *testStoreFactory {
	return &testStoreFactory{map[string]*TestStore{}}
}

func (f *testStoreFactory) CreateStore(ns string) ChunkStore {
	if cs, present := f.stores[ns]; present {
		return cs
	}
	f.stores[ns] = NewTestStore()
	return f.stores[ns]
}

func (f *testStoreFactory) Shutter() {
	f.stores = map[string]*TestStore{}
}
