package walk

import (
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Diff returns the hashes of the chunks reachable from 'to' that cannot be reached from 'from'
func Diff(from, to types.Value) (hashes []ref.Ref) {
	fromRefs := map[ref.Ref]bool{}
	WalkAll(from, func(v types.Value) {
		fromRefs[v.Ref()] = true
	})
	WalkAll(to, func(v types.Value) {
		if !fromRefs[v.Ref()] {
			hashes = append(hashes, v.Ref())
		}
	})
	return
}
