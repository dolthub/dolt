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

var ErrNothingToCollect = errors.New("no changes since last gc")

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
	// Get(), GetMany(), Has() and HasMany().
	Put(ctx context.Context, c Chunk) error

	// Returns the NomsVersion with which this ChunkSource is compatible.
	Version() string

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

// ChunkStoreGarbageCollector is a ChunkStore that supports garbage collection.
type ChunkStoreGarbageCollector interface {
	ChunkStore

	// MarkAndSweepChunks expects |keepChunks| to receive the chunk hashes
	// that should be kept in the chunk store. Once |keepChunks| is closed
	// and MarkAndSweepChunks returns, the chunk store will only have the
	// chunks sent on |keepChunks| and will have removed all other content
	// from the ChunkStore.
	MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash, dest ChunkStore) error
}

type PrefixChunkStore interface {
	ChunkStore

	ResolveShortHash(ctx context.Context, short []byte) (hash.Hash, error)
}

// GenerationalCS is an interface supporting the getting old gen and new gen chunk stores
type GenerationalCS interface {
	NewGen() ChunkStoreGarbageCollector
	OldGen() ChunkStoreGarbageCollector
}

var ErrUnsupportedOperation = errors.New("operation not supported")

var ErrGCGenerationExpired = errors.New("garbage collection generation expired")
