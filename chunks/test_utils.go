package chunks

import (
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func assertInputInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	data := s.Get(ref)
	assert.NotNil(data)
	assert.Equal(input, string(data))
}

func assertInputNotInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	data := s.Get(ref)
	assert.Nil(data)
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

func (s *TestStore) Get(ref ref.Ref) []byte {
	s.Reads++
	return s.MemoryStore.Get(ref)
}

func (s *TestStore) Has(ref ref.Ref) bool {
	return s.MemoryStore.Has(ref)
}

func (s *TestStore) Put() ChunkWriter {
	s.Writes++
	return s.MemoryStore.Put()
}
