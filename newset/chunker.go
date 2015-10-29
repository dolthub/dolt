package newset

import "github.com/attic-labs/noms/ref"

// Chunker is responsible for detecting chunk boundaries in a stream of refs.
type Chunker interface {
	// Adds a ref to the chunker, and returns whether it results in a chunk boundary.
	Add(r ref.Ref) bool
}

type chunkerFactory func() Chunker
