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
	"sync"
	"sync/atomic"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

type TestStorage struct {
	MemoryStorage
}

func (t *TestStorage) NewView() *TestStoreView {
	return &TestStoreView{ChunkStore: t.MemoryStorage.NewView()}
}

type TestStoreView struct {
	ChunkStore
	reads  int32
	hashes int32
	writes int32
}

var _ ChunkStoreGarbageCollector = &TestStoreView{}

func (s *TestStoreView) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	atomic.AddInt32(&s.reads, 1)
	return s.ChunkStore.Get(ctx, h)
}

func (s *TestStoreView) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error {
	atomic.AddInt32(&s.reads, int32(len(hashes)))
	return s.ChunkStore.GetMany(ctx, hashes, found)
}

func (s *TestStoreView) CacheHas(_ hash.Hash) bool {
	return false
}

func (s *TestStoreView) Has(ctx context.Context, h hash.Hash) (bool, error) {
	atomic.AddInt32(&s.hashes, 1)
	return s.ChunkStore.Has(ctx, h)
}

func (s *TestStoreView) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	atomic.AddInt32(&s.hashes, int32(len(hashes)))
	return s.ChunkStore.HasMany(ctx, hashes)
}

func (s *TestStoreView) Put(ctx context.Context, c Chunk, getAddrs GetAddrsCurry) error {
	atomic.AddInt32(&s.writes, 1)
	return s.ChunkStore.Put(ctx, c, getAddrs)
}

func (s *TestStoreView) BeginGC(keeper func(hash.Hash) bool) error {
	collector, ok := s.ChunkStore.(ChunkStoreGarbageCollector)
	if !ok {
		return ErrUnsupportedOperation
	}
	return collector.BeginGC(keeper)
}

func (s *TestStoreView) EndGC() {
	collector, ok := s.ChunkStore.(ChunkStoreGarbageCollector)
	if !ok {
		panic(ErrUnsupportedOperation)
	}
	collector.EndGC()
}

func (s *TestStoreView) MarkAndSweepChunks(ctx context.Context, hashes <-chan []hash.Hash, dest ChunkStore) error {
	collector, ok := s.ChunkStore.(ChunkStoreGarbageCollector)
	if !ok || dest != s {
		return ErrUnsupportedOperation
	}
	return collector.MarkAndSweepChunks(ctx, hashes, collector)
}

func (s *TestStoreView) GetChunkHashes(ctx context.Context, hashes chan<- hash.Hash, wg *sync.WaitGroup) int {
	//NM4 implement me
	panic("implement me")
}

func (s *TestStoreView) Reads() int {
	reads := atomic.LoadInt32(&s.reads)
	return int(reads)
}

func (s *TestStoreView) Hashes() int {
	hashes := atomic.LoadInt32(&s.hashes)
	return int(hashes)
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
