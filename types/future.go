package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// Future encapsulates a Value which may or may not be available yet.
type Future interface {
	// Returns the Ref of the value without fetching it.
	Ref() ref.Ref

	// Returns the Value if we already have it, nil otherwise.
	Val() Value

	// Fetch the Future value if necessary, then return it. Multiple calls to deref only result in one fetch.
	Deref(cs chunks.ChunkSource) (Value, error)

	// BUG 141. The lazy loading api is clearly not working.
	Release()
}

func futuresEqual(f1, f2 Future) bool {
	// If we already have both values, then use their Equals() methods since for primitives it is faster than computing a reference.
	if f1.Val() != nil && f2.Val() != nil {
		return f1.Val().Equals(f2.Val())
	} else {
		return f1.Ref() == f2.Ref()
	}
}

func futureEqualsValue(f Future, v Value) bool {
	d.Chk.NotNil(v)
	if f.Val() != nil {
		return f.Val().Equals(v)
	} else {
		return f.Ref() == v.Ref()
	}
}

func futureFromValue(v Value) Future {
	if r, ok := v.(Ref); ok {
		return &unresolvedFuture{ref: r.Ref()}
	} else {
		return resolvedFuture{v}
	}
}
