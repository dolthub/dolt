package walk

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// Difference returns the refs of the chunks reachable from 'big' that cannot be reached from 'small'
func Difference(small, big ref.Ref, cs chunks.ChunkSource) (refs []ref.Ref) {
	smallRefs := map[ref.Ref]bool{}
	if small != (ref.Ref{}) {
		All(small, cs, func(r ref.Ref) {
			smallRefs[r] = true
		})
	}
	Some(big, cs, func(r ref.Ref) (skip bool) {
		if skip = smallRefs[r]; !skip {
			refs = append(refs, r)
		}
		return
	})
	return
}
