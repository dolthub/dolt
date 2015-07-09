package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

func init() {
	Reffer = refferImpl
}

func refferImpl(v Value) ref.Ref {
	r, err := WriteValue(v, chunks.NopSink{})
	// This can never fail because NopSink doesn't write anywhere.
	Chk.Nil(err)
	return r
}
