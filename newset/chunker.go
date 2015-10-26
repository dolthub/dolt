package newset

import "github.com/attic-labs/noms/ref"

type Chunker interface {
	// Adds a ref to the chunker, and returns whether it results in a chunk boundary.
	Add(r ref.Ref) bool
	// Returns a new instance of this chunker's type. This is really a factory method hiding on an instance which is a bit icky.
	New() Chunker
}
