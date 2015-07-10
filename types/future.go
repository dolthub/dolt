package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// future is an internal helper that encapsulates a Value which may or may not be available yet.
type future interface {
	Ref() ref.Ref
	Equals(other future) bool
	Deref(cs chunks.ChunkSource) (Value, error)
}
