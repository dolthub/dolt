package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// SomeCallback takes a Future and returns a bool indicating whether the current walk should skip the tree descending from that Future.
type SomeCallback func(f Future) bool

type AllCallback func(f Future)

// Some recursively walks over all Values reachable from r and calls cb on them.
// If cb ever returns true, the walk will stop recursing that subtree.
func Some(r ref.Ref, cs chunks.ChunkSource, cb SomeCallback) {
	doTreeWalk(futureFromRef(r), cs, cb)
}

// All recursively walks over all Values reachable from r and calls cb on them.
func All(r ref.Ref, cs chunks.ChunkSource, cb AllCallback) {
	doTreeWalk(futureFromRef(r), cs, func(f Future) (skip bool) {
		cb(f)
		return
	})
}

func doTreeWalk(f Future, cs chunks.ChunkSource, cb SomeCallback) {
	doTreeWalk2(f, cs, cb, false)
}

func doTreeWalk2(f Future, cs chunks.ChunkSource, cb SomeCallback, skip bool) {
	// skip is set to true when we shoud skip the top level value and this is used
	// by compound lists which consists of other lists.
	if !skip && cb(f) {
		return
	}
	v := f.Deref(cs)

	switch v := v.(type) {
	case listLeaf:
		for _, v := range v.values {
			f := futureFromValue(v)
			doTreeWalk(f, cs, cb)
		}
	case Map:
		for _, e := range v.m {
			doTreeWalk(e.key, cs, cb)
			doTreeWalk(e.value, cs, cb)
		}
	case Set:
		for _, v := range v.data {
			f := futureFromValue(v)
			doTreeWalk(f, cs, cb)
		}
		// Note: no blob here because we're recursing the value tree, not the chunk tree. We treat each value as one thing, no matter how many chunks it is composed of.
	}
}
