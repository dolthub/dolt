package newset

import "github.com/attic-labs/noms/ref"

// fixedSizeChunker is a Chunker that always produces chunks of the same size.
type fixedSizeChunker struct {
	size   int
	cursor int
}

func newFixedSizeChunker(size int) Chunker {
	return &fixedSizeChunker{size: size}
}

func newFixedSizeChunkerFactory(size int) chunkerFactory {
	return func() Chunker { return newFixedSizeChunker(size) }
}

func (chunker *fixedSizeChunker) Add(r ref.Ref) bool {
	if chunker.cursor == chunker.size-1 {
		chunker.cursor = 0
		return true
	}
	chunker.cursor++
	return false
}
