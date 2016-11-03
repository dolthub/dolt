// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"sync"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// An in-memory implementation of store.ChunkStore. Useful mainly for tests.
type MemoryStore struct {
	data map[hash.Hash]Chunk
	memoryRootTracker
	mu sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (ms *MemoryStore) Get(h hash.Hash) Chunk {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if c, ok := ms.data[h]; ok {
		return c
	}
	return EmptyChunk
}

func (ms *MemoryStore) Has(r hash.Hash) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.data == nil {
		return false
	}
	_, ok := ms.data[r]
	return ok
}

func (ms *MemoryStore) Version() string {
	return constants.NomsVersion
}

func (ms *MemoryStore) Put(c Chunk) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.data == nil {
		ms.data = map[hash.Hash]Chunk{}
	}
	ms.data[c.Hash()] = c
}

func (ms *MemoryStore) PutMany(chunks []Chunk) (e BackpressureError) {
	for _, c := range chunks {
		ms.Put(c)
	}
	return
}

func (ms *MemoryStore) Len() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.data)
}

func (ms *MemoryStore) Close() error {
	return nil
}

func NewMemoryStoreFactory() Factory {
	return &MemoryStoreFactory{map[string]*MemoryStore{}}
}

type MemoryStoreFactory struct {
	stores map[string]*MemoryStore
}

func (f *MemoryStoreFactory) CreateStore(ns string) ChunkStore {
	if f.stores == nil {
		d.Panic("Cannot use MemoryStore after Shutter().")
	}
	if cs, present := f.stores[ns]; present {
		return cs
	}
	f.stores[ns] = NewMemoryStore()
	return f.stores[ns]
}

func (f *MemoryStoreFactory) Shutter() {
	f.stores = map[string]*MemoryStore{}
}
