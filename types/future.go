package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type Future interface {
	Ref() ref.Ref
	Equals(other Future) bool
	Deref(cs chunks.ChunkSource) (Value, error)
}
