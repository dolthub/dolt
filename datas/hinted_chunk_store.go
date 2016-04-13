package datas

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// Note that this doesn't actually implement the chunks.ChunkStore interface. This is by design, because we don't want to provide Has(), and because Put() is intended to be non-blocking and take 'hints', but I failed to come up with a different-but-still-relevant name.
type hintedChunkStore interface {
	hintedChunkSink
	chunks.RootTracker

	// Get retrieves the Chunk referenced by ref.
	Get(ref ref.Ref) chunks.Chunk
}

type hintedChunkSink interface {
	// Put writes c into the ChunkSink, using the provided hints to assist in validation. Validation requires checking that all refs embedded in c are themselves valid, which could be done by resolving each one. Instead, hints provides a (smaller) set of refs that point to chunks that themselves contain many of c's refs. Thus, by checking only the hinted chunks, c can be validated with fewer read operations.
	// c may or may not be persisted when Put() returns, but is guaranteed to be persistent after a call to Flush() or Close().
	Put(c chunks.Chunk, hints map[ref.Ref]struct{})

	// Flush causes enqueued Puts to be persisted.
	Flush()
	io.Closer
}

type naiveHintedChunkStore struct {
	cs chunks.ChunkStore
}

func (nhcs *naiveHintedChunkStore) Root() ref.Ref {
	return nhcs.cs.Root()
}

func (nhcs *naiveHintedChunkStore) UpdateRoot(current, last ref.Ref) bool {
	return nhcs.cs.UpdateRoot(current, last)
}

func (nhcs *naiveHintedChunkStore) Get(ref ref.Ref) chunks.Chunk {
	return nhcs.cs.Get(ref)
}

func (nhcs *naiveHintedChunkStore) Put(c chunks.Chunk, hints map[ref.Ref]struct{}) {
	nhcs.cs.Put(c)
}

func (nhcs *naiveHintedChunkStore) Flush() {
	return
}

func (nhcs *naiveHintedChunkStore) Close() error {
	nhcs.Flush()
	return nhcs.cs.Close()
}

type naiveHintedChunkSink struct {
	cs chunks.ChunkSink
}

func (nhcs *naiveHintedChunkSink) Put(c chunks.Chunk, hints map[ref.Ref]struct{}) {
	nhcs.cs.Put(c)
}

func (nhcs *naiveHintedChunkSink) Flush() {
	return
}

func (nhcs *naiveHintedChunkSink) Close() error {
	nhcs.Flush()
	return nhcs.cs.Close()
}
