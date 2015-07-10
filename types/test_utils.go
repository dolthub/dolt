package types

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type testStore struct {
	chunks.ChunkStore
	count int
}

func (s *testStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	s.count += 1
	return s.ChunkStore.Get(ref)
}
