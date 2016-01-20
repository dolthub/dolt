package chunks

import (
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func assertInputInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(ref)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", ref.String())
	assert.Equal(input, string(chunk.Data()))
}

func assertInputNotInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	data := s.Get(ref)
	assert.Nil(data, "Shouldn't have gotten data for %s", ref.String())
}

type TestStore struct {
	MemoryStore
	Reads  int
	Writes int
}

func NewTestStore() *TestStore {
	return &TestStore{
		MemoryStore: MemoryStore{
			mu: &sync.Mutex{},
		},
	}
}

func (s *TestStore) Get(ref ref.Ref) Chunk {
	s.Reads++
	return s.MemoryStore.Get(ref)
}

func (s *TestStore) Has(ref ref.Ref) bool {
	return s.MemoryStore.Has(ref)
}

func (s *TestStore) Put(c Chunk) {
	s.Writes++
	s.MemoryStore.Put(c)
}

type testStoreFactory struct {
	stores map[string]*TestStore
}

func NewTestStoreFactory() *testStoreFactory {
	return &testStoreFactory{map[string]*TestStore{}}
}

func (f *testStoreFactory) CreateNamespacedStore(ns string) ChunkStore {
	if cs, present := f.stores[ns]; present {
		return cs
	}
	f.stores[ns] = NewTestStore()
	return f.stores[ns]
}
