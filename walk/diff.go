package walk

import (
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// GetReachabilitySetDiff returns the hashes of the chunks reachable from 'big' that cannot be reached from 'small'
func GetReachabilitySetDiff(small, big types.Value) (hashes []ref.Ref) {
	smallRefs := map[ref.Ref]bool{}
	WalkAll(small, func(v types.Value) {
		smallRefs[v.Ref()] = true
	})
	WalkAll(big, func(v types.Value) {
		if !smallRefs[v.Ref()] {
			hashes = append(hashes, v.Ref())
		}
	})
	return
}
