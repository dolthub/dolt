package types

import "github.com/attic-labs/noms/chunks"

func futureFromValue(v Value) future {
	return resolvedFuture{v}
}

type resolvedFuture struct {
	Value
}

func (rf resolvedFuture) Equals(other future) bool {
	// If we already have both values, then use their Equals() methods since for primitives it is faster than computing a reference.
	if rf.Value != nil && other.Val() != nil {
		return rf.Value.Equals(other.Val())
	} else {
		return rf.Ref() == other.Ref()
	}
}

func (rf resolvedFuture) Val() Value {
	return rf.Value
}

func (rf resolvedFuture) Deref(cs chunks.ChunkSource) (Value, error) {
	return rf.Value, nil
}
