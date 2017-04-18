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
	cs   chunks.ChunkStore
	once sync.Once
	mu   sync.Mutex
	cc   *completenessChecker
}

func newLocalBatchStore(cs chunks.ChunkStore) *localBatchStore {
	return &localBatchStore{cs: cs, cc: newCompletenessChecker()}
}

// Get checks the internal Chunk cache, proxying to the backing ChunkStore if
// not present.
func (lbs *localBatchStore) Get(h hash.Hash) chunks.Chunk {
	lbs.once.Do(lbs.expectVersion)
	return lbs.cs.Get(h)
}

func (lbs *localBatchStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	lbs.cs.GetMany(hashes, foundChunks)
}

// SchedulePut calls Put on the underlying ChunkStore and adds any refs in c
// to a pool of unresolved refs which are validated against the underlying
// ChunkStore during Flush() or UpdateRoot().
func (lbs *localBatchStore) SchedulePut(c chunks.Chunk) {
	lbs.once.Do(lbs.expectVersion)
	lbs.cs.Put(c)
	lbs.mu.Lock()
	defer lbs.mu.Unlock()
	lbs.cc.AddRefs(types.DecodeValue(c, nil))
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

// UpdateRoot flushes outstanding writes to the backing ChunkStore before
// updating its Root, because it's almost certainly the case that the caller
// wants to point that root at some recently-Put Chunk.
func (lbs *localBatchStore) UpdateRoot(current, last hash.Hash) bool {
	lbs.once.Do(lbs.expectVersion)
	lbs.Flush()
	return lbs.cs.UpdateRoot(current, last)
}

func (lbs *localBatchStore) Flush() {
	lbs.once.Do(lbs.expectVersion)
	func() {
		lbs.mu.Lock()
		defer lbs.mu.Unlock()
		lbs.cc.PanicIfDangling(lbs.cs)
	}()
	lbs.cs.Flush()
}

// Close closes the underlying ChunkStore.
func (lbs *localBatchStore) Close() error {
	lbs.Flush()
	return lbs.cs.Close()
}
