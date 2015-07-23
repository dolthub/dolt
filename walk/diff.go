package walk

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// GetReachabilitySetDiff returns the refs of the chunks reachable from 'big' that cannot be reached from 'small'
func GetReachabilitySetDiff(small, big ref.Ref, cs chunks.ChunkSource) (refs []ref.Ref) {
	smallRefs := map[ref.Ref]bool{}
	All(small, cs, func(r ref.Ref) {
		smallRefs[r] = true
	})
	Some(big, cs, func(r ref.Ref) (skip bool) {
		if skip = smallRefs[r]; !skip {
			refs = append(refs, r)
		}
		return
	})
	return
}
