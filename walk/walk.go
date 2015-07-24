package walk

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// SomeCallback takes a ref.Ref and returns a bool indicating whether
// the current walk should skip the tree descending from value.
type SomeCallback func(r ref.Ref) bool

// AllCallback takes a ref and processes it.
type AllCallback func(r ref.Ref)

// Some recursively walks over all ref.Refs reachable from r and calls cb on them.
// If cb ever returns true, the walk will stop recursing on the current ref.
func Some(r ref.Ref, cs chunks.ChunkSource, cb SomeCallback) {
	doTreeWalk(r, cs, cb)
}

// All recursively walks over all ref.Refs reachable from r and calls cb on them.
func All(r ref.Ref, cs chunks.ChunkSource, cb AllCallback) {
	doTreeWalk(r, cs, func(r ref.Ref) (skip bool) {
		cb(r)
		return
	})
}

func doTreeWalk(r ref.Ref, cs chunks.ChunkSource, cb SomeCallback) {
	if cb(r) {
		return
	}
	v, err := types.ReadValue(r, cs)
	dbg.Chk.NoError(err)
	for _, cf := range v.Chunks() {
		doTreeWalk(cf.Ref(), cs, cb)
	}
}
