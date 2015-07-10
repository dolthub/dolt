package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func futureFromRef(ref ref.Ref) future {
	return &unresolvedFuture{ref: ref}
}

type unresolvedFuture struct {
	val Value
	ref ref.Ref
}

func (f *unresolvedFuture) Val() Value {
	return f.val
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
