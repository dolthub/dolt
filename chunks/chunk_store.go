package chunks

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ChunkStore is the core storage abstraction in noms. We can put data anyplace we have a ChunkStore implementation for.
type ChunkStore interface {
	io.Closer
	ChunkSource
	ChunkSink
	RootTracker
}

// RootTracker allows querying and management of the root of an entire tree of references. The "root" is the single mutable variable in a ChunkStore. It can store any ref, but it is typically used by higher layers (such as DataStore) to store a ref to a value that represents the current state and entire history of a datastore.
type RootTracker interface {
	Root() ref.Ref
	UpdateRoot(current, last ref.Ref) bool
}

// ChunkSource is a place to get chunks from.
type ChunkSource interface {
	// Get gets a reader for the value of the Ref in the store. If the ref is absent from the store nil is returned.
	Get(ref ref.Ref) io.ReadCloser

	// Returns true iff the value at the address |ref| is contained in the source
	Has(ref ref.Ref) bool
}

// ChunkSink is a place to put chunks.
type ChunkSink interface {
	Put() ChunkWriter
}

/*
	Chunk Serialization:
		Chunk 0
		Chunk 1
		 ..
		Chunk N

	Chunk:
		Len   // 4-byte int
		Data  // len(Data) == Len
*/

func Serialize(w io.Writer, refs map[ref.Ref]bool, cs ChunkSource) {
	// TODO: If ChunkSource could provide the length of a chunk without having to buffer it, this could be completely streaming.
	chunk := &bytes.Buffer{}
	for r, _ := range refs {
		chunk.Reset()
		r := cs.Get(r)
		if r == nil {
			continue
		}

		_, err := io.Copy(chunk, r)
		d.Chk.NoError(err)

		// Because of chunking at higher levels, no chunk should never be more than 4GB
		chunkSize := uint32(len(chunk.Bytes()))
		err = binary.Write(w, binary.LittleEndian, chunkSize)
		d.Chk.NoError(err)

		n, err := io.Copy(w, chunk)
		d.Chk.NoError(err)
		d.Chk.Equal(uint32(n), chunkSize)
	}
}

func Deserialize(r io.Reader, cs ChunkSink) {
	for {
		chunkSize := uint32(0)
		err := binary.Read(r, binary.LittleEndian, &chunkSize)
		if err == io.EOF {
			break
		}
		d.Chk.NoError(err)

		w := cs.Put()
		_, err = io.CopyN(w, r, int64(chunkSize))
		d.Chk.NoError(err)
		w.Close()
	}
}

// NewFlags creates a new instance of Flags, which declares a number of ChunkStore-related command-line flags using the golang flag package. Call this before flag.Parse().
func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

// NewFlagsWithPrefix creates a new instance of Flags with the names of all flags declared therein prefixed by the given string.
func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		httpFlags(prefix),
		levelDBFlags(prefix),
		memoryFlags(prefix),
		nopFlags(prefix),
	}
}

// Flags abstracts away definitions for and handling of command-line flags for all ChunkStore implementations.
type Flags struct {
	http   httpStoreFlags
	ldb    ldbStoreFlags
	memory memoryStoreFlags
	nop    nopStoreFlags
}

// CreateStore creates a ChunkStore implementation based on the values of command-line flags.
func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.http.createStore(); cs != nil {
	} else if cs = f.ldb.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.nop.createStore(); cs != nil {
	}
	return cs
}
