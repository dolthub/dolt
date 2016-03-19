package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// ValueStore is currently used only for tests in this package.
type ValueStore struct {
	cs chunks.ChunkStore
}

// NewTestValueStore creates a simple struct that satisfies ValueReadWriter and is backed by a chunks.TestStore.
func NewTestValueStore() *ValueStore {
	return &ValueStore{chunks.NewTestStore()}
}

func newValueStore(cs chunks.ChunkStore) *ValueStore {
	return &ValueStore{cs}
}

// ReadValue reads and decodes a value from vrw. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (vrw *ValueStore) ReadValue(r ref.Ref) Value {
	return DecodeChunk(vrw.cs.Get(r), vrw)
}

func (vrw *ValueStore) WriteValue(v Value) ref.Ref {
	chunk := EncodeValue(v, vrw)
	vrw.cs.Put(chunk)
	return chunk.Ref()
}
