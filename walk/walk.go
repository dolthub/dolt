package walk

import "github.com/attic-labs/noms/types"

// WalkCallback takes a types.Value and returns a bool indicating whether
// the current walk should skip the tree descending from value.
type WalkCallback func(value types.Value) bool

// WalkAllCallback takes a value and processes it.
type WalkAllCallback func(value types.Value)

// Walk recursively walks over all types.Values reachable from v and calls cb on them.
// If cb ever returns true, the tree walk will stop recursing on the current value.
func Walk(v types.Value, cb WalkCallback) {
	doTreeWalk(v, cb)
}

func WalkAll(v types.Value, cb WalkAllCallback) {
	doTreeWalk(v, func(value types.Value) (skip bool) {
		cb(value)
		return
	})
}

func doTreeWalk(v types.Value, cb WalkCallback) {
	if cb(v) {
		return
	}
	switch v := v.(type) {
	case types.List:
		for i := uint64(0); i < v.Len(); i++ {
			doTreeWalk(v.Get(i), cb)
		}
	case types.Map:
		v.Iter(func(key, value types.Value) (stop bool) {
			doTreeWalk(key, cb)
			doTreeWalk(value, cb)
			return
		})
	case types.Set:
		v.Iter(func(value types.Value) (stop bool) {
			doTreeWalk(value, cb)
			return
		})
	default:
	}
}
