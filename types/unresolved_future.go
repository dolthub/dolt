package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func FutureFromRef(ref ref.Ref) Future {
	return &unresolvedFuture{ref: ref}
}

type unresolvedFuture struct {
	ref ref.Ref
	val Value
}

func (f *unresolvedFuture) Deref(cs chunks.ChunkSource) (Value, error) {
	if f.val != nil {
		return f.val, nil
	}

	val, err := ReadValue(f.ref, cs)
	if err != nil {
		return nil, err
	}

	f.val = val
	return f.val, nil
}

func (f *unresolvedFuture) Ref() ref.Ref {
	return f.ref
}

func (f *unresolvedFuture) Equals(other Future) bool {
	return f.ref == other.Ref()
}
