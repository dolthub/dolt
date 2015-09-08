package types

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func futureFromRef(ref ref.Ref) Future {
	return &unresolvedFuture{ref: ref, mu: &sync.Mutex{}}
}

type unresolvedFuture struct {
	val Value
	ref ref.Ref
	mu  *sync.Mutex
}

func (f *unresolvedFuture) Val() Value {
	return f.val
}

func (f *unresolvedFuture) Deref(cs chunks.ChunkSource) Value {
	f.mu.Lock()
	defer f.mu.Unlock()
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
