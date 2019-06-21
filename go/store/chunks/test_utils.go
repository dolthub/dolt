// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	"github.com/liquidata-inc/ld/dolt/go/store/go/hash"
	"github.com/stretchr/testify/assert"
)

func assertInputInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(context.Background(), h)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func assertInputNotInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(context.Background(), h)
	assert.True(chunk.IsEmpty(), "Shouldn't get non-empty chunk for %s: %v", h.String(), chunk)
}

type TestStorage struct {
	MemoryStorage
}

func (t *TestStorage) NewView() *TestStoreView {
	return &TestStoreView{ChunkStore: t.MemoryStorage.NewView()}
}

type TestStoreView struct {
	ChunkStore
	Reads  int
	Hases  int
	Writes int
}

func (s *TestStoreView) Get(ctx context.Context, h hash.Hash) Chunk {
	s.Reads++
	return s.ChunkStore.Get(ctx, h)
}

func (s *TestStoreView) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *Chunk) {
	s.Reads += len(hashes)
	s.ChunkStore.GetMany(ctx, hashes, foundChunks)
}

func (s *TestStoreView) Has(ctx context.Context, h hash.Hash) bool {
	s.Hases++
	return s.ChunkStore.Has(ctx, h)
}

func (s *TestStoreView) HasMany(ctx context.Context, hashes hash.HashSet) hash.HashSet {
	s.Hases += len(hashes)
	return s.ChunkStore.HasMany(ctx, hashes)
}

func (s *TestStoreView) Put(ctx context.Context, c Chunk) {
	s.Writes++
	s.ChunkStore.Put(ctx, c)
}

type TestStoreFactory struct {
	stores map[string]*TestStorage
}

func NewTestStoreFactory() *TestStoreFactory {
	return &TestStoreFactory{map[string]*TestStorage{}}
}

func (f *TestStoreFactory) CreateStore(ns string) ChunkStore {
	if f.stores == nil {
		d.Panic("Cannot use TestStoreFactory after Shutter().")
	}
	if ts, present := f.stores[ns]; present {
		return ts.NewView()
	}
	f.stores[ns] = &TestStorage{}
	return f.stores[ns].NewView()
}

func (f *TestStoreFactory) Shutter() {
	f.stores = nil
}
