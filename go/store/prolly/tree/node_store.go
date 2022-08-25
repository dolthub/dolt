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

package tree

import (
	"context"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	cacheSize = 256 * 1024 * 1024
)

// NodeStore reads and writes prolly tree Nodes.
type NodeStore interface {
	// Read reads a prolly tree Node from the store.
	Read(ctx context.Context, ref hash.Hash) (Node, error)

	// ReadMany reads many prolly tree Nodes from the store.
	ReadMany(ctx context.Context, refs hash.HashSlice) ([]Node, error)

	// Write writes a prolly tree Node to the store.
	Write(ctx context.Context, nd Node) (hash.Hash, error)

	// Pool returns a buffer pool.
	Pool() pool.BuffPool

	// Format returns the types.NomsBinFormat of this NodeStore.
	Format() *types.NomsBinFormat
}

type nodeStore struct {
	store chunks.ChunkStore
	cache chunkCache
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
		return NodeFromBytes(c.Data())
	}

	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return Node{}, err
	}
	assertTrue(c.Size() > 0, "empty chunk returned from ChunkStore")

	ns.cache.insert(c)

	return NodeFromBytes(c.Data())
}

// ReadMany implements NodeStore.
func (ns nodeStore) ReadMany(ctx context.Context, refs hash.HashSlice) ([]Node, error) {
	found := make(map[hash.Hash]chunks.Chunk)
	gets := hash.HashSet{}

	for _, r := range refs {
		c, ok := ns.cache.get(r)
		if ok {
			found[r] = c
		} else {
			gets.Insert(r)
		}
	}

	mu := new(sync.Mutex)
	err := ns.store.GetMany(ctx, gets, func(ctx context.Context, chunk *chunks.Chunk) {
		mu.Lock()
		defer mu.Unlock()
		found[chunk.Hash()] = *chunk
	})
	if err != nil {
		return nil, err
	}

	nodes := make([]Node, len(refs))
	for i, r := range refs {
		c, ok := found[r]
		if ok {
			nodes[i], err = NodeFromBytes(c.Data())
			if err != nil {
				return nil, err
			}
		}
	}
	return nodes, nil
}

// Write implements NodeStore.
func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd.bytes())
	assertTrue(c.Size() > 0, "cannot write empty chunk to ChunkStore")

	if err := ns.store.Put(ctx, c); err != nil {
		return hash.Hash{}, err
	}
	ns.cache.insert(c)
	return c.Hash(), nil
}

// Pool implements NodeStore.
func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}

func (ns nodeStore) Format() *types.NomsBinFormat {
	nbf, err := types.GetFormatForVersionString(ns.store.Version())
	if err != nil {
		panic(err)
	}
	return nbf
}
