// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type validatingChunkStore struct {
	chunks.ChunkStore
	mu sync.Mutex
	cc *completenessChecker
}

func newValidatingChunkStore(cs chunks.ChunkStore) *validatingChunkStore {
	return &validatingChunkStore{ChunkStore: cs, cc: newCompletenessChecker()}
}

// Put calls Put on the underlying ChunkStore and adds any refs in c
// to a pool of unresolved refs which are validated against the underlying
// ChunkStore during Flush() or Commit().
func (vcs *validatingChunkStore) Put(c chunks.Chunk) {
	vcs.ChunkStore.Put(c)
	vcs.mu.Lock()
	defer vcs.mu.Unlock()
	vcs.cc.AddRefs(types.DecodeValue(c, nil))
}

// PutMany calls PutMany on the underlying ChunkStore and adds any refs in c
// to a pool of unresolved refs which are validated against the underlying
// ChunkStore during Flush() or Commit().
func (vcs *validatingChunkStore) PutMany(chunks []chunks.Chunk) {
	vcs.ChunkStore.PutMany(chunks)
	vcs.mu.Lock()
	defer vcs.mu.Unlock()
	for _, c := range chunks {
		vcs.cc.AddRefs(types.DecodeValue(c, nil))
	}
}

// Commit validates pending chunks for ref-completeness before calling
// Commit() on the underlying ChunkStore.
func (vcs *validatingChunkStore) Commit(current, last hash.Hash) bool {
	vcs.validate()
	return vcs.ChunkStore.Commit(current, last)
}

// Flush validates pending chunks for ref-completeness before calling
// Flush() on the underlying ChunkStore.
func (vcs *validatingChunkStore) Flush() {
	vcs.validate()
	vcs.ChunkStore.Flush()
}

func (vcs *validatingChunkStore) validate() {
	vcs.mu.Lock()
	defer vcs.mu.Unlock()
	vcs.cc.PanicIfDangling(vcs.ChunkStore)
}
