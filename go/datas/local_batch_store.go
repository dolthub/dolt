// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"io"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/golang/snappy"
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
		vbs:           types.NewValidatingBatchingSink(cs, types.NewTypeCache()),
		hints:         types.Hints{},
		hashes:        hash.HashSet{},
		mu:            &sync.Mutex{},
	}
}

func (lbs *localBatchStore) IsValidating() bool {
	return true
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
	d.PanicIfTrue(constants.NomsVersion != dataVersion, "SDK version %s incompatible with data of version %s", constants.NomsVersion, dataVersion)
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

	serializedChunks, pw := io.Pipe()
	errChan := make(chan error)
	go func() {
		err := lbs.unwrittenPuts.ExtractChunks(lbs.hashes, pw)
		// The ordering of these is important. Close the pipe, and only THEN block on errChan.
		pw.Close()
		errChan <- err
		close(errChan)
	}()

	lbs.vbs.Prepare(lbs.hints)
	var bpe chunks.BackpressureError
	chunkChan := make(chan *chunks.Chunk, 16)
	go chunks.DeserializeToChan(snappy.NewReader(serializedChunks), chunkChan)
	for c := range chunkChan {
		if bpe == nil {
			bpe = lbs.vbs.Enqueue(*c)
		} else {
			bpe = append(bpe, c.Hash())
		}
		// If a previous Enqueue() errored, we still need to drain chunkChan
		// TODO: what about having DeserializeToChan take a 'done' channel to stop it?
	}
	d.PanicIfError(<-errChan)
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

// Close closes the underlying ChunkStore
func (lbs *localBatchStore) Close() error {
	lbs.Flush()
	lbs.unwrittenPuts.Destroy()
	return lbs.cs.Close()
}
