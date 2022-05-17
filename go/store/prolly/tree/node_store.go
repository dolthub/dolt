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
		return NodeFromBytes(c.Data()), nil
	}

	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return Node{}, err
	}
	assertTrue(c.Size() > 0)

	ns.cache.insert(c)

	return NodeFromBytes(c.Data()), err
}

// Write implements NodeStore.
func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd.bytes())
	assertTrue(c.Size() > 0)

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
	// todo(andy): read from |ns.store|
	return types.Format_DOLT_1
}
