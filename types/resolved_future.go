package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func futureFromValue(v Value) Future {
	return resolvedFuture{v}
}

type resolvedFuture struct {
	val Value
}

func (rf resolvedFuture) Ref() ref.Ref {
	return rf.val.Ref()
}

func (rf resolvedFuture) Val() Value {
	return rf.val
}

func (rf resolvedFuture) Deref(cs chunks.ChunkSource) (Value, error) {
	return rf.val, nil
}

func (rf resolvedFuture) Release() {
}
