package types

import "github.com/attic-labs/noms/chunks"

func futureFromValue(v Value) future {
	return resolvedFuture{v}
}

type resolvedFuture struct {
	Value
}

func (rf resolvedFuture) Equals(other future) bool {
	// TODO: We can avoid the hashes if we know that both us and the other guy are primitives.
	return rf.Ref() == other.Ref()
}

func (rf resolvedFuture) Deref(cs chunks.ChunkSource) (Value, error) {
	return rf.Value, nil
}
