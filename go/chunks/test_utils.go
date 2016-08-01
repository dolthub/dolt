// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func assertInputInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(h)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func assertInputNotInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	data := s.Get(h)
	assert.Nil(data, "Shouldn't have gotten data for %s", h.String())
}

type TestStore struct {
	MemoryStore
	Reads  int
	Hases  int
	Writes int
}

func NewTestStore() *TestStore {
	return &TestStore{}
}

func (s *TestStore) Get(h hash.Hash) Chunk {
	s.Reads++
	return s.MemoryStore.Get(h)
}

func (s *TestStore) Has(h hash.Hash) bool {
	s.Hases++
	return s.MemoryStore.Has(h)
}

func (s *TestStore) Put(c Chunk) {
	s.Writes++
	s.MemoryStore.Put(c)
}

func (s *TestStore) PutMany(chunks []Chunk) (e BackpressureError) {
	for _, c := range chunks {
		s.Put(c)
	}
	return
}

// TestStoreFactory is public, and exposes Stores to ensure that test code can directly query instances vended by this factory.
type TestStoreFactory struct {
	Stores map[string]*TestStore
}

func NewTestStoreFactory() *TestStoreFactory {
	return &TestStoreFactory{map[string]*TestStore{}}
}

func (f *TestStoreFactory) CreateStore(ns string) ChunkStore {
	if cs, present := f.Stores[ns]; present {
		return cs
	}
	f.Stores[ns] = NewTestStore()
	return f.Stores[ns]
}

func (f *TestStoreFactory) Shutter() {
	f.Stores = map[string]*TestStore{}
}
