package chunks

import (
	"flag"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// NopStore is a no-write implementation of chunks.ChunkStore.
// Get() doesn't work, because the data isn't ever written, but other stuff works
type NopStore struct {
	memoryRootTracker
}

// Get panics in NopStore! Since the data wasn't stored, you sure can't Get it.
func (ms *NopStore) Get(ref ref.Ref) Chunk {
	d.Chk.Fail("Whoops, you shouldn't have called this!")
	return EmptyChunk
}

func (ms *NopStore) Has(ref ref.Ref) bool {
	return false
}

func (ms *NopStore) Put(c Chunk) {
}

func (ms *NopStore) Close() error {
	return nil
}

type NopStoreFlags struct {
	use *bool
}

func NopFlags(prefix string) NopStoreFlags {
	return NopStoreFlags{
		flag.Bool(prefix+"nop", false, "use a /dev/null-esque chunkstore"),
	}
}

func (f NopStoreFlags) CreateStore() ChunkStore {
	if *f.use {
		return &NopStore{}
	} else {
		return nil
	}
}
