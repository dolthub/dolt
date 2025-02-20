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
	"bytes"
	"context"
	"github.com/dolthub/dolt/go/store/val"
	"sync"

	"github.com/dolthub/dolt/go/store/prolly/message"

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
	val.ValueStore

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

	BlobBuilder() *BlobBuilder
	PutBlobBuilder(*BlobBuilder)

	// Delete any cached chunks associated with this NodeStore.
	// Used by GC during safepoint establishment to ensure deleted
	// chunks do not float around in the application layer after GC
	// completes.
	PurgeCaches()
}

type nodeStore struct {
	store chunks.ChunkStore
	cache nodeCache
	bp    pool.BuffPool
	bbp   *sync.Pool
}

var _ NodeStore = nodeStore{}

var sharedCache = newChunkCache(cacheSize)

var sharedPool = pool.NewBuffPool()

var blobBuilderPool = sync.Pool{
	New: func() any {
		return mustNewBlobBuilder(DefaultFixedChunkLength)
	},
}

// NewNodeStore makes a new NodeStore.
func NewNodeStore(cs chunks.ChunkStore) NodeStore {
	return nodeStore{
		store: cs,
		cache: sharedCache,
		bp:    sharedPool,
		bbp:   &blobBuilderPool,
	}
}

// Read implements NodeStore.
func (ns nodeStore) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	n, ok := ns.cache.get(ref)
	if ok {
		return n, nil
	}

	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return Node{}, err
	}
	assertTrue(c.Size() > 0, "empty chunk returned from ChunkStore")

	n, _, err = NodeFromBytes(c.Data())
	if err != nil {
		return Node{}, err
	}
	ns.cache.insert(ref, n)

	return n, nil
}

// ReadMany implements NodeStore.
func (ns nodeStore) ReadMany(ctx context.Context, addrs hash.HashSlice) ([]Node, error) {
	found := make(map[hash.Hash]Node)
	gets := hash.HashSet{}

	for _, r := range addrs {
		n, ok := ns.cache.get(r)
		if ok {
			found[r] = n
		} else {
			gets.Insert(r)
		}
	}

	var nerr error
	mu := new(sync.Mutex)
	err := ns.store.GetMany(ctx, gets, func(ctx context.Context, chunk *chunks.Chunk) {
		n, _, err := NodeFromBytes(chunk.Data())
		if err != nil {
			nerr = err
		}
		mu.Lock()
		found[chunk.Hash()] = n
		mu.Unlock()
	})
	if err == nil {
		err = nerr
	}
	if err != nil {
		return nil, err
	}

	var ok bool
	nodes := make([]Node, len(addrs))
	for i, addr := range addrs {
		nodes[i], ok = found[addr]
		if ok {
			ns.cache.insert(addr, nodes[i])
		}
	}
	return nodes, nil
}

// Write implements NodeStore.
func (ns nodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd.bytes())
	assertTrue(c.Size() > 0, "cannot write empty chunk to ChunkStore")

	getAddrs := func(ch chunks.Chunk) chunks.GetAddrsCb {
		return func(ctx context.Context, addrs hash.HashSet, exists chunks.PendingRefExists) (err error) {
			err = message.WalkAddresses(ctx, ch.Data(), func(ctx context.Context, a hash.Hash) error {
				if !exists(a) {
					addrs.Insert(a)
				}
				return nil
			})
			return
		}
	}

	if err := ns.store.Put(ctx, c, getAddrs); err != nil {
		return hash.Hash{}, err
	}
	ns.cache.insert(c.Hash(), nd)
	return c.Hash(), nil
}

// Pool implements NodeStore.
func (ns nodeStore) Pool() pool.BuffPool {
	return ns.bp
}

// BlobBuilder implements NodeStore.
func (ns nodeStore) BlobBuilder() *BlobBuilder {
	bb := ns.bbp.Get().(*BlobBuilder)
	bb.SetNodeStore(ns)
	return bb
}

// PutBlobBuilder implements NodeStore.
func (ns nodeStore) PutBlobBuilder(bb *BlobBuilder) {
	bb.Reset()
	ns.bbp.Put(bb)
}

func (ns nodeStore) Format() *types.NomsBinFormat {
	nbf, err := types.GetFormatForVersionString(ns.store.Version())
	if err != nil {
		panic(err)
	}
	return nbf
}

func (ns nodeStore) PurgeCaches() {
	ns.cache.purge()
}

func (ns nodeStore) ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error) {
	return NewByteArray(h, ns).ToBytes(ctx)
}

func (ns nodeStore) WriteBytes(ctx context.Context, b []byte) (hash.Hash, error) {
	_, h, err := SerializeBytesToAddr(ctx, ns, bytes.NewReader(b), len(b))
	return h, err
}

var _ val.ValueStore = nodeStore{}
