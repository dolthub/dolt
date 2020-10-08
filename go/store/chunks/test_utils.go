// Copyright 2019 Liquidata, Inc.
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
	"sync/atomic"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func assertInputInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk, err := s.Get(context.Background(), h)
	assert.NoError(err)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func assertInputNotInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk, err := s.Get(context.Background(), h)
	assert.NoError(err)
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
	reads  int32
	hases  int32
	writes int32
}

var _ ChunkStoreGarbageCollector = &TestStoreView{}

func (s *TestStoreView) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	atomic.AddInt32(&s.reads, 1)
	return s.ChunkStore.Get(ctx, h)
}

func (s *TestStoreView) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan<- *Chunk) error {
	atomic.AddInt32(&s.reads, int32(len(hashes)))
	return s.ChunkStore.GetMany(ctx, hashes, foundChunks)
}

func (s *TestStoreView) Has(ctx context.Context, h hash.Hash) (bool, error) {
	atomic.AddInt32(&s.hases, 1)
	return s.ChunkStore.Has(ctx, h)
}

func (s *TestStoreView) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	atomic.AddInt32(&s.hases, int32(len(hashes)))
	return s.ChunkStore.HasMany(ctx, hashes)
}

func (s *TestStoreView) Put(ctx context.Context, c Chunk) error {
	atomic.AddInt32(&s.writes, 1)
	return s.ChunkStore.Put(ctx, c)
}

func (s *TestStoreView) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan hash.Hash, errChan chan<- error) error {
	collector, ok := s.ChunkStore.(ChunkStoreGarbageCollector)

	if !ok {
		return ErrUnsupportedOperation
	}

	return collector.MarkAndSweepChunks(ctx, last, keepChunks, errChan)
}

func (s *TestStoreView) Reads() int {
	reads := atomic.LoadInt32(&s.reads)
	return int(reads)
}

func (s *TestStoreView) Hases() int {
	hases := atomic.LoadInt32(&s.hases)
	return int(hases)
}

func (s *TestStoreView) Writes() int {
	writes := atomic.LoadInt32(&s.writes)
	return int(writes)
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
