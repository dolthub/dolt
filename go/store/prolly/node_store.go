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

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

// NodeStore reads and writes prolly tree Nodes.
type NodeStore interface {

	// Read reads a prolly tree Node from the store.
	Read(ctx context.Context, ref hash.Hash) (Node, error)

	// Write writes a prolly tree Node to the store.
	Write(ctx context.Context, nd Node) (hash.Hash, error)

	// Pool returns a buffer pool.
	Pool() pool.BuffPool
}

type nodeStore struct {
	store chunks.ChunkStore
	bp    pool.BuffPool
}

var _ NodeStore = nodeStore{}

var sharedPool = pool.NewBuffPool()

// NewNodeStore makes a new NodeStore.
func NewNodeStore(cs chunks.ChunkStore) NodeStore {
	return nodeStore{store: cs, bp: sharedPool}
}

// Read implements NodeStore.
func (ns nodeStore) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	c, err := ns.store.Get(ctx, ref)
	return c.Data(), err
}

// Write implements NodeStore.
func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd)
	err := ns.store.Put(ctx, c)
	return c.Hash(), err
}

// Pool implements NodeStore.
func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}
