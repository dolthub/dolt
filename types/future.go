package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

// future is an internal helper that encapsulates a Value which may or may not be available yet.
type future interface {
	// Returns the Ref of the value without fetching it.
	Ref() ref.Ref

	// Returns the Value if we already have it, nil otherwise.
	Val() Value

	// Fetch the future value if necessary, then return it. Multiple calls to deref only result in one fetch.
	Deref(cs chunks.ChunkSource) (Value, error)
}

func futuresEqual(f1, f2 future) bool {
	// If we already have both values, then use their Equals() methods since for primitives it is faster than computing a reference.
	if f1.Val() != nil && f2.Val() != nil {
		return f1.Val().Equals(f2.Val())
	} else {
		return f1.Ref() == f2.Ref()
	}
}

func futureEqualsValue(f future, v Value) bool {
	Chk.NotNil(v)
	if f.Val() != nil {
		return f.Val().Equals(v)
	} else {
		return f.Ref() == v.Ref()
	}
}
