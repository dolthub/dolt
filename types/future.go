package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// TODO: Seems like maybe this shouldn't be public for now?
type Future interface {
	Ref() ref.Ref
	Equals(other Future) bool
	Deref(cs chunks.ChunkSource) (Value, error)
}
