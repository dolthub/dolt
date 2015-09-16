package chunks

import (
	"bytes"
	"crypto/sha1"
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
	Get(ref ref.Ref) []byte

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
		Ref   // 20-byte sha1 hash
		Len   // 4-byte int
		Data  // len(Data) == Len
*/

type Chunk struct {
	Ref  ref.Ref
	Data []byte
}

// Serialize reads |chunks|, serializing each to |w|. The caller is responsible for closing |chunks|, after which, Serialize will finish and then send on |done|. If an error occurs, it will be sent |err| immediately before completion.
func Serialize(w io.Writer, chunks <-chan Chunk) (done <-chan struct{}, err <-chan interface{}) {
	dc := make(chan struct{})
	er := make(chan interface{}, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				er <- r
			}

			dc <- struct{}{}
		}()

		for chunk := range chunks {
			d.Chk.NotNil(chunk.Data)

			digest := chunk.Ref.Digest()
			n, err := io.Copy(w, bytes.NewReader(digest[:]))
			d.Chk.NoError(err)
			d.Chk.Equal(int64(sha1.Size), n)

			// Because of chunking at higher levels, no chunk should never be more than 4GB
			chunkSize := uint32(len(chunk.Data))
			err = binary.Write(w, binary.LittleEndian, chunkSize)
			d.Chk.NoError(err)

			n, err = io.Copy(w, bytes.NewReader(chunk.Data))
			d.Chk.NoError(err)
			d.Chk.Equal(uint32(n), chunkSize)
		}
	}()

	return dc, er
}

// Deserialize reads off of |r|, sending chunks to |chunks|. When EOF is reached, it closes |chunks|. If an error is encountered, it is sent on |err|.
func Deserialize(r io.Reader) (chunks <-chan Chunk, err <-chan interface{}) {
	ch := make(chan Chunk)
	er := make(chan interface{}, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				er <- r
			}
			close(ch)
		}()

		for {
			digest := ref.Sha1Digest{}
			n, err := io.ReadFull(r, digest[:])
			if err == io.EOF {
				break
			}
			d.Chk.NoError(err)
			d.Chk.Equal(int(sha1.Size), n)

			chunkSize := uint32(0)
			err = binary.Read(r, binary.LittleEndian, &chunkSize)
			d.Chk.NoError(err)

			chunk := &bytes.Buffer{}
			_, err = io.CopyN(chunk, r, int64(chunkSize))
			d.Chk.NoError(err)

			ch <- Chunk{ref.New(digest), chunk.Bytes()}
		}
	}()
	return ch, er
}

// NewFlags creates a new instance of Flags, which declares a number of ChunkStore-related command-line flags using the golang flag package. Call this before flag.Parse().
func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

// NewFlagsWithPrefix creates a new instance of Flags with the names of all flags declared therein prefixed by the given string.
func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		levelDBFlags(prefix),
		memoryFlags(prefix),
		nopFlags(prefix),
	}
}

// Flags abstracts away definitions for and handling of command-line flags for all ChunkStore implementations.
type Flags struct {
	ldb    ldbStoreFlags
	memory memoryStoreFlags
	nop    nopStoreFlags
}

// CreateStore creates a ChunkStore implementation based on the values of command-line flags.
func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.ldb.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.nop.createStore(); cs != nil {
	}
	return cs
}
