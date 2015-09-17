package chunks

import (
	"flag"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// NopStore is a no-write implementation of chunks.ChunkStore.
// Get() doesn't work, because the data isn't ever written, but other stuff works
type NopStore struct {
	memoryRootTracker
}

// Get panics in NopStore! Since the data wasn't stored, you sure can't Get it.
func (ms *NopStore) Get(ref ref.Ref) io.ReadCloser {
	d.Chk.Fail("Whoops, you shouldn't have called this!")
	return nil
}

func (ms *NopStore) Has(ref ref.Ref) bool {
	return false
}

func (ms *NopStore) Put() ChunkWriter {
	return NewChunkWriter(ms.write)
}

func (ms *NopStore) write(ref ref.Ref, data []byte) {}

func (ms *NopStore) Close() error {
	return nil
}

type nopStoreFlags struct {
	use *bool
}

func nopFlags(prefix string) nopStoreFlags {
	return nopStoreFlags{
		flag.Bool(prefix+"nop", false, "use a /dev/null-esque chunkstore"),
	}
}

func (f nopStoreFlags) createStore() ChunkStore {
	if *f.use {
		return &NopStore{}
	} else {
		return nil
	}
}
