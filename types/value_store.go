package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// ValueStore is currently used only for tests in this pacakge.
type ValueStore struct {
	cs chunks.ChunkStore
}

func newValueStore(cs chunks.ChunkStore) *ValueStore {
	return &ValueStore{cs}
}

func NewTestValueStore() *ValueStore {
	return &ValueStore{chunks.NewTestStore()}
}

// ReadValue reads and decodes a value from vrw. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (vrw *ValueStore) ReadValue(r ref.Ref) Value {
	c := vrw.cs.Get(r)
	return DecodeChunk(c, vrw)
}

func (vrw *ValueStore) WriteValue(v Value) ref.Ref {
	return WriteValue(v, vrw.cs)
}
