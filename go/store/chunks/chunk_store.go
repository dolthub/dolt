// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

// A ChunkStore's |ExclusiveAccessMode| can indicate to a client what type of
// exclusive access and concurrency control are support by the ChunkStore, and
// for a ChunkStore instance which the client is holding, what level of access
// it has. Typically, a ChunkStore will be in Mode_Shared, which means multiple
// processes can concurrently write to the chunk store and the chunk store can
// change out from under the process. Other possible modes, currently only used
// by local ChunkStores using the chunk journal in go/store/nbs are
// Mode_ReadOnly, which means the journal was opened in read only mode and all
// attempts to write will fail, or Mode_Exclusive, which means the journal was
// opened successfully in write mode and no other processes will write to it
// while this ChunkStore is open.
type ExclusiveAccessMode uint8

// Note: the order of these constants is relied on in nbs/GenerationalNBS. The
// order should go from least restricted access to most restricted access. If a
// client has many stores with different levels of accses, a common question
// they want to answer is: what is the most restricted access across all of
// these stores.
const (
	ExclusiveAccessMode_Shared = iota
	ExclusiveAccessMode_Exclusive
	ExclusiveAccessMode_ReadOnly
)

var ErrNothingToCollect = errors.New("no changes since last gc")

// GetAddrsCurry returns a function that will add a chunk's child
// references to a HashSet. The intermediary lets us build a single
// HashSet per memTable.
type GetAddrsCurry func(c Chunk) GetAddrsCb

// GetAddrsCb adds the refs for a pre-specified chunk to |addrs|
type GetAddrsCb func(ctx context.Context, addrs hash.HashSet, exists PendingRefExists) error

type PendingRefExists func(hash.Hash) bool

func NoopPendingRefExists(_ hash.Hash) bool { return false }

// ChunkStore is the core storage abstraction in noms. We can put data
// anyplace we have a ChunkStore implementation for.
type ChunkStore interface {
	// Get the Chunk for the value of the hash in the store. If the hash is
	// absent from the store EmptyChunk is returned.
	Get(ctx context.Context, h hash.Hash) (Chunk, error)

	// GetMany gets the Chunks with |hashes| from the store. On return,
	// |foundChunks| will have been fully sent all chunks which have been
	// found. Any non-present chunks will silently be ignored.
	GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error

	// Returns true iff the value at the address |h| is contained in the
	// store
	Has(ctx context.Context, h hash.Hash) (bool, error)

	// Returns a new HashSet containing any members of |hashes| that are
	// absent from the store.
	HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error)

	// Put caches c in the ChunkSource. Upon return, c must be visible to
	// subsequent Get and Has calls, but must not be persistent until a call
	// to Flush(). Put may be called concurrently with other calls to Put(),
	// Get(), GetMany(), Has() and HasMany(). Will return an error if the
	// addrs returned by `getAddrs` are absent from the chunk store.
	Put(ctx context.Context, c Chunk, getAddrs GetAddrsCurry) error

	// Returns the NomsBinFormat with which this ChunkSource is compatible.
	Version() string

	// Returns the current access mode for this opened ChunkStore.
	AccessMode() ExclusiveAccessMode

	// Rebase brings this ChunkStore into sync with the persistent storage's
	// current root.
	Rebase(ctx context.Context) error

	// Root returns the root of the database as of the time the ChunkStore
	// was opened or the most recent call to Rebase.
	Root(ctx context.Context) (hash.Hash, error)

	// Commit atomically attempts to persist all novel Chunks and update the
	// persisted root hash from last to current (or keeps it the same).
	// If last doesn't match the root in persistent storage, returns false.
	Commit(ctx context.Context, current, last hash.Hash) (bool, error)

	// Stats may return some kind of struct that reports statistics about the
	// ChunkStore instance. The type is implementation-dependent, and impls
	// may return nil
	Stats() interface{}

	// StatsSummary may return a string containing summarized statistics for
	// this ChunkStore. It must return "Unsupported" if this operation is not
	// supported.
	StatsSummary() string

	// PersistGhostHashes is used to persist a set of addresses that are known to exist, but
	// are not currently stored here. Only the GenerationalChunkStore implementation allows use of this method, as
	// shallow clones are only allowed in local copies currently. Note that at the application level, the only
	// hashes which can be ghosted are commit ids, but the chunk store doesn't know what those are.
	PersistGhostHashes(ctx context.Context, refs hash.HashSet) error

	// Close tears down any resources in use by the implementation. After
	// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
	// Close() concurrently with any other ChunkStore method; behavior is
	// undefined and probably crashy.
	io.Closer
}

type DebugLogger interface {
	Logf(fmt string, args ...interface{})
}

type LoggingChunkStore interface {
	ChunkStore
	SetLogger(logger DebugLogger)
}

// The sentinel error returned by BeginGC(addChunk) implementations
// indicating that the store must wait until the GC is over and then
// ensure that the attempted write makes it into the new data for the chunk
// store.

var ErrAddChunkMustBlock = errors.New("chunk keeper: add chunk must block")

// ChunkStoreGarbageCollector is a ChunkStore that supports garbage collection.
type ChunkStoreGarbageCollector interface {
	ChunkStore

	// After BeginGC returns, every newly written chunk or root should be
	// passed to the provided |addChunk| function. This behavior must
	// persist until EndGC is called. MarkAndSweepChunks will only ever
	// be called bracketed between a BeginGC and an EndGC call.
	//
	// If during processing the |addChunk| function returns the
	// |true|, then the ChunkStore must block the write until |EndGC| is
	// called. At that point, the ChunkStore is responsible for ensuring
	// that the chunk which it was attempting to write makes it into chunk
	// store.
	//
	// This function should not block indefinitely and should return an
	// error if a GC is already in progress.
	BeginGC(addChunk func(hash.Hash) bool) error

	// EndGC indicates that the GC is over. The previously provided
	// addChunk function must not be called after this function.
	EndGC()

	// MarkAndSweepChunks is expected to read chunk addresses off of
	// |hashes|, which represent chunks which should be copied into the
	// provided |dest| store.  Once |hashes| is closed,
	// MarkAndSweepChunks is expected to update the contents of the store
	// to only include the chunk whose addresses which were sent along on
	// |hashes|.
	//
	// This behavior is a little different for ValueStore.GC()'s
	// interactions with generational stores. See ValueStore and
	// NomsBlockStore/GenerationalNBS for details.
	MarkAndSweepChunks(ctx context.Context, hashes <-chan []hash.Hash, dest ChunkStore) error

	// Count returns the number of chunks in the store.
	Count() (uint32, error)

	// IterateAllChunks iterates over all chunks in the store, calling the provided callback for each chunk. This is
	// a wrapper over the internal chunkSource.iterateAllChunks() method.
	IterateAllChunks(context.Context, func(chunk Chunk)) error
}

type PrefixChunkStore interface {
	ChunkStore

	ResolveShortHash(ctx context.Context, short []byte) (hash.Hash, error)
}

// GenerationalCS is an interface supporting the getting old gen and new gen chunk stores
type GenerationalCS interface {
	NewGen() ChunkStoreGarbageCollector
	OldGen() ChunkStoreGarbageCollector
	GhostGen() ChunkStore
}

var ErrUnsupportedOperation = errors.New("operation not supported")

var ErrGCGenerationExpired = errors.New("garbage collection generation expired")

type PushConcurrencyControl int8

const (
	PushConcurrencyControl_IgnoreWorkingSet = iota
	PushConcurrencyControl_AssertWorkingSet = iota
)

type ConcurrencyControlChunkStore interface {
	PushConcurrencyControl() PushConcurrencyControl
}

func GetPushConcurrencyControl(cs ChunkStore) PushConcurrencyControl {
	if cs, ok := cs.(ConcurrencyControlChunkStore); ok {
		return cs.PushConcurrencyControl()
	}
	return PushConcurrencyControl_IgnoreWorkingSet
}
