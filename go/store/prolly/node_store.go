// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	cacheSize = 256 * 1024 * 1024
)

// NodeStore reads and writes prolly tree Nodes.
type NodeStore interface {
	// Read reads a prolly tree Node from the store.
	Read(ctx context.Context, ref hash.Hash) (Node, error)

	// Write writes a prolly tree Node to the store.
	Write(ctx context.Context, nd Node) (hash.Hash, error)

	FetchMany(ctx context.Context, refs hash.HashSet) error

	// Pool returns a buffer pool.
	Pool() pool.BuffPool

	// Format returns the types.NomsBinFormat of this NodeStore.
	Format() *types.NomsBinFormat
}

type nodeStore struct {
	store chunks.ChunkStore
	cache *chunkCache
	bp    pool.BuffPool
}

var _ NodeStore = nodeStore{}

var sharedCache = newChunkCache(cacheSize)

var sharedPool = pool.NewBuffPool()

// NewNodeStore makes a new NodeStore.
func NewNodeStore(cs chunks.ChunkStore) NodeStore {
	return nodeStore{
		store: cs,
		cache: sharedCache,
		bp:    sharedPool,
	}
}

// Read implements NodeStore.
func (ns nodeStore) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	c, ok := ns.cache.get(ref)
	if ok {
		return mapNodeFromBytes(c.Data()), nil
	}

	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return Node{}, err
	}
	ns.cache.insert(c)

	return mapNodeFromBytes(c.Data()), err
}

// Write implements NodeStore.
func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd.bytes())
	if err := ns.store.Put(ctx, c); err != nil {
		return hash.Hash{}, err
	}
	ns.cache.insert(c)
	return c.Hash(), nil
}

func (ns nodeStore) FetchMany(ctx context.Context, refs hash.HashSet) error {
	absent := refs.Copy()
	for h := range absent {
		if _, ok := ns.cache.get(h); ok {
			delete(absent, h)
		}
	}

	return ns.store.GetMany(ctx, absent, func(ctx context.Context, chunk *chunks.Chunk) {
		ns.cache.insert(*chunk)
	})
}

// Pool implements NodeStore.
func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}

func (ns nodeStore) Format() *types.NomsBinFormat {
	// todo(andy): read from |ns.store|
	return types.Format_DOLT_1
}

type centry struct {
	c    chunks.Chunk
	i    int
	prev *centry
	next *centry
}

type chunkCache struct {
	mu     *sync.Mutex
	chunks map[hash.Hash]*centry
	head   *centry
	sz     int
	maxSz  int
	rev    int
}

func newChunkCache(maxSize int) *chunkCache {
	return &chunkCache{
		&sync.Mutex{},
		make(map[hash.Hash]*centry),
		nil,
		0,
		maxSize,
		0,
	}
}

func removeFromCList(e *centry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = e
	e.next = e
}

func (mc *chunkCache) moveToFront(e *centry) {
	e.i = mc.rev
	mc.rev++
	if mc.head == e {
		return
	}
	if mc.head != nil {
		removeFromCList(e)
		e.next = mc.head
		e.prev = mc.head.prev
		mc.head.prev = e
		e.prev.next = e
	}
	mc.head = e
}

func (mc *chunkCache) get(h hash.Hash) (chunks.Chunk, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if e, ok := mc.chunks[h]; ok {
		mc.moveToFront(e)
		return e.c, true
	} else {
		return chunks.EmptyChunk, false
	}
}

func (mc *chunkCache) getMany(hs hash.HashSet) ([]chunks.Chunk, hash.HashSet) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	absent := make(map[hash.Hash]struct{})
	var found []chunks.Chunk
	for h, _ := range hs {
		if e, ok := mc.chunks[h]; ok {
			mc.moveToFront(e)
			found = append(found, e.c)
		} else {
			absent[h] = struct{}{}
		}
	}
	return found, absent
}

func (mc *chunkCache) insert(c chunks.Chunk) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.addIfAbsent(c)
}

func (mc *chunkCache) insertMany(cs []chunks.Chunk) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, c := range cs {
		mc.addIfAbsent(c)
	}
}

func (mc *chunkCache) addIfAbsent(c chunks.Chunk) {
	if e, ok := mc.chunks[c.Hash()]; !ok {
		e := &centry{c, 0, nil, nil}
		e.next = e
		e.prev = e
		mc.moveToFront(e)
		mc.chunks[c.Hash()] = e
		mc.sz += c.Size()
		mc.shrinkToMaxSz()
	} else {
		mc.moveToFront(e)
	}
}

func (mc *chunkCache) Len() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return len(mc.chunks)
}

func (mc *chunkCache) Size() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.sz
}

func (mc *chunkCache) shrinkToMaxSz() {
	for mc.sz > mc.maxSz {
		if mc.head != nil {
			t := mc.head.prev
			removeFromCList(t)
			if t == mc.head {
				mc.head = nil
			}
			delete(mc.chunks, t.c.Hash())
			mc.sz -= t.c.Size()
		} else {
			panic("cache is empty but cache size is > than max size")
		}
	}
}

func (mc *chunkCache) sanityCheck() {
	if mc.head != nil {
		p := mc.head.next
		i := 1
		sz := mc.head.c.Size()
		lasti := mc.head.i
		for p != mc.head {
			i++
			sz += p.c.Size()
			if p.i >= lasti {
				panic("encountered lru list entry with higher rev later in the list.")
			}
			p = p.next
		}
		if i != len(mc.chunks) {
			panic(fmt.Sprintf("cache lru list has different size than cache.chunks. %d vs %d", i, len(mc.chunks)))
		}
		if sz != mc.sz {
			panic("entries reachable from lru list have different size than cache.sz.")
		}
		j := 1
		p = mc.head.prev
		for p != mc.head {
			j++
			p = p.prev
		}
		if j != i {
			panic("length of list backwards is not equal to length of list forward")
		}
	} else {
		if len(mc.chunks) != 0 {
			panic("lru list is empty but mc.chunks is not")
		}
		if mc.sz != 0 {
			panic("lru list is empty but mc.sz is not 0")
		}
	}
}
