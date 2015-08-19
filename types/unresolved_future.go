package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func futureFromRef(ref ref.Ref) Future {
	return &unresolvedFuture{ref: ref}
}

type unresolvedFuture struct {
	val Value
	ref ref.Ref
}

func (f *unresolvedFuture) Val() Value {
	return f.val
}

func (f *unresolvedFuture) Deref(cs chunks.ChunkSource) Value {
	if f.val != nil {
		return f.val
	}

	f.val = ReadValue(f.ref, cs)
	return f.val
}

func (f *unresolvedFuture) Ref() ref.Ref {
	return f.ref
}

func (f *unresolvedFuture) Release() {
	f.val = nil
}
