package chunks

import (
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func assertInputInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	reader, err := s.Get(ref)
	assert.NoError(err)
	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))
}

func assertInputNotInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	reader, err := s.Get(ref)
	assert.NoError(err)
	assert.Nil(reader)
}

type TestStore struct {
	MemoryStore
	Reads  int
	Writes int
}

func (s *TestStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	s.Reads++
	return s.MemoryStore.Get(ref)
}

func (s *TestStore) Put() ChunkWriter {
	s.Writes++
	return s.MemoryStore.Put()
}
