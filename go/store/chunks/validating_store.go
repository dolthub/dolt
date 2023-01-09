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
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

type validatingStore struct {
	ChunkStore
	data chunkMap
}

func NewValidatingChunkStore(cs ChunkStore) ChunkStore {
	return validatingStore{ChunkStore: cs, data: newChunkMap()}
}

func (cs validatingStore) Get(ctx context.Context, h hash.Hash) (c Chunk, err error) {
	c, err = cs.ChunkStore.Get(ctx, h)
	if !c.IsEmpty() && !cs.data.has(h) {
		panic(fmt.Sprintf("missing chunk for address %s", h.String()))
	}
	return
}

func (cs validatingStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error {
	for h := range hashes {
		if h.IsEmpty() {
			continue
		}
		if !cs.data.has(h) {
			panic(fmt.Sprintf("missing chunk for address %s", h.String()))
		}
	}
	return cs.ChunkStore.GetMany(ctx, hashes, found)
}

func (cs validatingStore) Has(ctx context.Context, h hash.Hash) (ok bool, err error) {
	ok, err = cs.ChunkStore.Has(ctx, h)
	if err != nil {
		return false, err
	}
	ok2 := cs.data.has(h)
	if ok != ok2 {
		panic(fmt.Sprintf("expected equal (%t != %t)", ok, ok2))
	}
	return
}

func (cs validatingStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	absent, err = cs.ChunkStore.HasMany(ctx, hashes)
	if err != nil {
		return nil, err
	}
	absent2 := hash.NewHashSet()
	for h := range hashes {
		if !cs.data.has(h) {
			absent2.Insert(h)
		}
	}
	if !absent.Equals(absent2) {
		panic(fmt.Sprintf("expected equal (%s != %s)", absent.String(), absent2.String()))
	}
	return
}

func (cs validatingStore) Put(ctx context.Context, c Chunk) error {
	cs.data.put(c)
	return cs.ChunkStore.Put(ctx, c)
}

func (cs validatingStore) MarkAndSweepChunks(ctx context.Context, root hash.Hash, keepers <-chan []hash.Hash, store ChunkStore) error {
	save := make(map[hash.Hash]Chunk)
	for _, h := range cs.data.hashes() {
		save[h], _ = cs.data.get(h)
		cs.data.delete(h)
	}

	keepers2 := make(chan []hash.Hash)
	defer close(keepers2)
	go func() {
		_ = cs.ChunkStore.(ChunkStoreGarbageCollector).MarkAndSweepChunks(ctx, root, keepers2, store)
	}()

	for {
		select {
		case hs, ok := <-keepers:
			if !ok {
				return nil
			}
			for _, h := range hs {
				cs.data.put(save[h])
			}
			keepers2 <- hs

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type chunkMap struct {
	data map[hash.Hash]Chunk
	lock *sync.Mutex
}

func newChunkMap() chunkMap {
	return chunkMap{
		data: make(map[hash.Hash]Chunk),
		lock: new(sync.Mutex),
	}
}

func (m chunkMap) has(h hash.Hash) (ok bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok = m.data[h]
	return
}

func (m chunkMap) get(h hash.Hash) (c Chunk, ok bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	c, ok = m.data[h]
	return
}

func (m chunkMap) put(c Chunk) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[c.Hash()] = c
}

func (m chunkMap) delete(h hash.Hash) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.data, h)
}

func (m chunkMap) iter(cb func(c Chunk)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, c := range m.data {
		cb(c)
	}
}

func (m chunkMap) hashes() (hh []hash.Hash) {
	m.lock.Lock()
	defer m.lock.Unlock()
	hh = make([]hash.Hash, 0, len(m.data))
	for h := range m.data {
		hh = append(hh, h)
	}
	return
}
