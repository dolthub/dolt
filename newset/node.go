package newset

import "github.com/attic-labs/noms/ref"

// A node is intended to map onto a single chunk, which compose together to form a Set. It defines primitive operations which Set can use to implement the set interface.
type node interface {
	length() uint64 // expensive, but should we cache it for the public API?
	start() ref.Ref
	has(r ref.Ref) bool // expensive
	appendRef(first, r ref.Ref) node
	ref() ref.Ref
	iter(func(int, ref.Ref)) // expensive
	fmt(indent int) string   // expensive
}
