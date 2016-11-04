// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type localBatchStore struct {
	cs            chunks.ChunkStore
	unwrittenPuts *orderedChunkCache
	vbs           *types.ValidatingBatchingSink
	hints         types.Hints
	hashes        hash.HashSet
	mu            *sync.Mutex
	once          sync.Once
}

func newLocalBatchStore(cs chunks.ChunkStore) *localBatchStore {
	return &localBatchStore{
		cs:            cs,
		unwrittenPuts: newOrderedChunkCache(),
		vbs:           types.NewValidatingBatchingSink(cs),
		hints:         types.Hints{},
		hashes:        hash.HashSet{},
		mu:            &sync.Mutex{},
	}
}

// Get checks the internal Chunk cache, proxying to the backing ChunkStore if not present.
func (lbs *localBatchStore) Get(h hash.Hash) chunks.Chunk {
	lbs.once.Do(lbs.expectVersion)
	if pending := lbs.unwrittenPuts.Get(h); !pending.IsEmpty() {
		return pending
	}
	return lbs.cs.Get(h)
}

// Has checks the internal Chunk cache, proxying to the backing ChunkStore if not present.
func (lbs *localBatchStore) Has(h hash.Hash) bool {
	lbs.once.Do(lbs.expectVersion)
	if lbs.unwrittenPuts.has(h) {
		return true
	}
	return lbs.cs.Has(h)
}

// SchedulePut simply calls Put on the underlying ChunkStore, and ignores hints.
func (lbs *localBatchStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
	lbs.once.Do(lbs.expectVersion)

	lbs.unwrittenPuts.Insert(c, refHeight)
	lbs.mu.Lock()
	defer lbs.mu.Unlock()
	lbs.hashes.Insert(c.Hash())
	lbs.AddHints(hints)
}

func (lbs *localBatchStore) expectVersion() {
	dataVersion := lbs.cs.Version()
	if constants.NomsVersion != dataVersion {
		d.Panic("SDK version %s incompatible with data of version %s", constants.NomsVersion, dataVersion)
	}
}

func (lbs *localBatchStore) Root() hash.Hash {
	lbs.once.Do(lbs.expectVersion)
	return lbs.cs.Root()
}

// UpdateRoot flushes outstanding writes to the backing ChunkStore before updating its Root, because it's almost certainly the case that the caller wants to point that root at some recently-Put Chunk.
func (lbs *localBatchStore) UpdateRoot(current, last hash.Hash) bool {
	lbs.once.Do(lbs.expectVersion)
	lbs.Flush()
	return lbs.cs.UpdateRoot(current, last)
}

func (lbs *localBatchStore) AddHints(hints types.Hints) {
	for h := range hints {
		lbs.hints[h] = struct{}{}
	}
}

func (lbs *localBatchStore) Flush() {
	lbs.once.Do(lbs.expectVersion)

	chunkChan := make(chan *chunks.Chunk, 128)
	go func() {
		err := lbs.unwrittenPuts.ExtractChunks(lbs.hashes, chunkChan)
		d.Chk.NoError(err)
		close(chunkChan)
	}()

	lbs.vbs.Prepare(lbs.hints)
	var bpe chunks.BackpressureError
	for c := range chunkChan {
		if bpe == nil {
			dc := lbs.vbs.DecodeUnqueued(c)
			bpe = lbs.vbs.Enqueue(*dc.Chunk, *dc.Value)
		} else {
			bpe = append(bpe, c.Hash())
		}
	}
	if bpe == nil {
		bpe = lbs.vbs.Flush()
	}
	// Should probably do a thing with bpe. Will need to keep track of chunk hashes that are SechedulePut'd in order to do this :-/
	if bpe != nil {
		d.PanicIfError(bpe) // guarded because if bpe == nil, this still fires for some reason. Maybe something to do with custom error type??
	}
	lbs.unwrittenPuts.Clear(lbs.hashes)
	lbs.hashes = hash.HashSet{}
	lbs.hints = types.Hints{}
}

// FlushAndDestroyWithoutClose flushes lbs and destroys its cache of unwritten chunks. It's needed because LocalDatabase wraps a localBatchStore around a ChunkStore that's used by a separate BatchStore, so calling Close() on one is semantically incorrect while it still wants to use the other.
func (lbs *localBatchStore) FlushAndDestroyWithoutClose() {
	lbs.Flush()
	lbs.unwrittenPuts.Destroy()
}

// Destroy blows away lbs' cache of unwritten chunks without flushing. Used when the owning Database is closing and it isn't semantically correct to flush.
func (lbs *localBatchStore) Destroy() {
	lbs.unwrittenPuts.Destroy()
}

// Close is supposed to close the underlying ChunkStore, but the only place localBatchStore is currently used wants to keep the underlying ChunkStore open after it's done with lbs. Hence, the above method and the panic() here.
func (lbs *localBatchStore) Close() error {
	panic("Unreached")
}
