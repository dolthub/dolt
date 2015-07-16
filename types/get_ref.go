package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var getRefOverride func(v Value) ref.Ref

func getRef(v Value) ref.Ref {
	if getRefOverride != nil {
		return getRefOverride(v)
	}
	return getRefNoOverride(v)
}

func getRefNoOverride(v Value) ref.Ref {
	r, err := WriteValue(v, chunks.NopSink{})
	// This can never fail because NopSink doesn't write anywhere.
	Chk.Nil(err)
	return r
}

func ensureRef(r *ref.Ref, v Value) ref.Ref {
	if *r == (ref.Ref{}) {
		*r = getRef(v)
	}
	return *r
}
