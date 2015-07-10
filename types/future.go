package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// future is an internal helper that encapsulates a Value which may or may not be available yet.
type future interface {
	// Returns the Ref of the value without fetching it.
	Ref() ref.Ref

	// Returns the Value if we already have it, nil otherwise.
	Val() Value

	// Determines if the two future values are equal without fetching either.
	Equals(other future) bool

	// Fetch the future value if necessary, then return it. Multiple calls to deref only result in one fetch.
	Deref(cs chunks.ChunkSource) (Value, error)
}
