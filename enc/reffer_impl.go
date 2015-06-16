package enc

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func init() {
	types.Reffer = refferImpl
}

func refferImpl(v types.Value) ref.Ref {
	r, err := WriteValue(v, chunks.NopSink{})
	// This can never fail because NopSink doesn't write anywhere.
	Chk.Nil(err)
	return r
}
