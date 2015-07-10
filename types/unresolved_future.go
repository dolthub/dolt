package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func futureFromRef(ref ref.Ref) future {
	return &unresolvedFuture{ref: ref}
}

type unresolvedFuture struct {
	resolvedFuture
	ref ref.Ref
}

func (f *unresolvedFuture) Deref(cs chunks.ChunkSource) (Value, error) {
	if f.Val() != nil {
		return f.Val(), nil
	}

	val, err := ReadValue(f.ref, cs)
	if err != nil {
		return nil, err
	}

	f.resolvedFuture.Value = val
	return f.resolvedFuture.Value, nil
}

func (f *unresolvedFuture) Ref() ref.Ref {
	return f.ref
}
