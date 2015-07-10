package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var getRef = func(v Value) ref.Ref {
	r, err := WriteValue(v, chunks.NopSink{})
	// This can never fail because NopSink doesn't write anywhere.
	Chk.Nil(err)
	return r
}

var ensureRef = func(r *ref.Ref, v Value) ref.Ref {
	if *r == (ref.Ref{}) {
		*r = getRef(v)
	}
	return *r
}
