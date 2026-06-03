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
	"io"
	"sync"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/memlimit"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// NodeStore reads and writes prolly tree Nodes.
// TODO(next): put Compare on this interface, maybe just for adaptive types
type NodeStore interface {
	val.ValueStore

	// Read reads a prolly tree Node from the store.
	Read(ctx context.Context, ref hash.Hash) (*Node, error)

	// ReadMany reads many prolly tree Nodes from the store.
	ReadMany(ctx context.Context, refs hash.HashSlice) ([]*Node, error)

	// Write writes a prolly tree Node to the store.
	Write(ctx context.Context, nd *Node) (hash.Hash, error)

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

var _ NodeStore = &nodeStore{}

var (
	sharedCacheOnce sync.Once
	sharedCache     nodeCache
)

func getSharedCache() nodeCache {
	sharedCacheOnce.Do(func() {
		sharedCache = newChunkCache(int(memlimit.NodeCacheSize()))
	})
	return sharedCache
}

var sharedPool = pool.NewBuffPool()

var blobBuilderPool = sync.Pool{
	New: func() any {
		return mustNewBlobBuilder(DefaultFixedChunkLength)
	},
}

// NewNodeStore makes a new NodeStore.
func NewNodeStore(cs chunks.ChunkStore) NodeStore {
	return &nodeStore{
		store: cs,
		cache: getSharedCache(),
		bp:    sharedPool,
		bbp:   &blobBuilderPool,
	}
}

// Read implements NodeStore.
func (ns *nodeStore) Read(ctx context.Context, ref hash.Hash) (*Node, error) {
	n, ok := ns.cache.get(ref)
	if ok {
		return n, nil
	}

	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	assertTrue(c.Size() > 0, "empty chunk returned from ChunkStore")

	n, _, err = NodeFromChunk(&c)
	if err != nil {
		return nil, err
	}
	ns.cache.insert(ref, n)

	return n, nil
}

// ReadMany implements NodeStore.
func (ns *nodeStore) ReadMany(ctx context.Context, addrs hash.HashSlice) ([]*Node, error) {
	found := make(map[hash.Hash]*Node)
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
		n, _, err := NodeFromChunk(chunk)
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
	nodes := make([]*Node, len(addrs))
	for i, addr := range addrs {
		nodes[i], ok = found[addr]
		if ok {
			ns.cache.insert(addr, nodes[i])
		}
	}
	return nodes, nil
}

// Write implements NodeStore.
func (ns *nodeStore) Write(ctx context.Context, nd *Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd.bytes())
	assertTrue(c.Size() > 0, "cannot write empty chunk to ChunkStore")

	getAddrs := func(ch chunks.Chunk) chunks.InsertAddrsCb {
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
func (ns *nodeStore) Pool() pool.BuffPool {
	return ns.bp
}

// BlobBuilder implements NodeStore.
func (ns *nodeStore) BlobBuilder() *BlobBuilder {
	bb := ns.bbp.Get().(*BlobBuilder)
	bb.SetNodeStore(ns)
	return bb
}

// PutBlobBuilder implements NodeStore.
func (ns *nodeStore) PutBlobBuilder(bb *BlobBuilder) {
	bb.Reset()
	ns.bbp.Put(bb)
}

func (ns *nodeStore) Format() *types.NomsBinFormat {
	nbf, err := types.GetFormatForVersionString(ns.store.Version())
	if err != nil {
		panic(err)
	}
	return nbf
}

func (ns *nodeStore) PurgeCaches() {
	ns.cache.purge()
}

func (ns *nodeStore) ReadBytes(ctx context.Context, h hash.Hash) (result []byte, err error) {
	n, err := ns.Read(ctx, h)
	if err != nil {
		return nil, err
	}

	err = WalkNodes(ctx, n, ns, func(ctx context.Context, n *Node) error {
		if n.IsLeaf() {
			result = append(result, n.GetValue(0)...)
		}
		return nil
	})
	return result, err
}

// CompareJsonAdaptiveValues implements val.JsonAdaptiveValueComparator. The work is done by
// IndexedJsonDocument.Compare, which compares two JSON documents chunk-by-chunk and applies
// MySQL's JSON ordering rules.
func (ns *nodeStore) CompareJsonAdaptiveValues(ctx context.Context, l, r val.AdaptiveValue) (int, error) {
	return compareJsonAdaptiveValues(ctx, ns, l, r)
}

func (ns *nodeStore) WriteBytes(ctx context.Context, b []byte) (hash.Hash, error) {
	_, h, err := SerializeBytesToAddr(ctx, ns, bytes.NewReader(b), len(b))
	return h, err
}

// CompareAdaptive implements val.ValueStore
func (ns *nodeStore) CompareAdaptive(ctx context.Context, l val.AdaptiveValue, r val.AdaptiveValue, enc val.Encoding) (int, error) {
	if enc == val.JsonAdaptiveEnc {
		return ns.CompareJsonAdaptiveValues(ctx, l, r)
	}

	// If both values are inline we can compare their payloads without touching the ValueStore.
	lPayload, lInline := val.InlineValueBytes(l)
	rPayload, rInline := val.InlineValueBytes(r)
	if lInline && rInline {
		return bytes.Compare(lPayload, rPayload), nil
	}

	differ, err := newBlobChunkDiffer(ctx, ns, l, r)
	if err != nil {
		return 0, err
	}

	return compareChunkDiffer(ctx, differ)
}

func compareChunkDiffer(ctx context.Context, d chunkDiffer) (int, error) {
	lChunk, rChunk, err := d.Next(ctx)
	if err == io.EOF {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return bytes.Compare(lChunk, rChunk), nil
}

// CompareAdaptiveCollatedStrings implements val.ValueStore
func (ns *nodeStore) CompareAdaptiveCollatedStrings(ctx context.Context, l, r val.AdaptiveValue, collation sql.CollationID) (int, error) {
	// no collation uses the faster byte order comparison
	if collation == sql.Collation_Unspecified {
		return ns.CompareAdaptive(ctx, l, r, val.StringAdaptiveEnc)
	}

	lPayload, lInline := val.InlineValueBytes(l)
	rPayload, rInline := val.InlineValueBytes(r)
	if lInline && rInline {
		return val.CompareCollatedStrings(collation, lPayload, rPayload), nil
	}

	differ, err := newBlobChunkDiffer(ctx, ns, l, r)
	if err != nil {
		return 0, err
	}

	return compareCollatedChunkDiffer(ctx, differ, collation)
}

// compareCollatedChunkDiffer is the streaming version of CompareCollatedStrings, getting just the diff between two
// values when comparing them.
func compareCollatedChunkDiffer(ctx context.Context, d chunkDiffer, collation sql.CollationID) (int, error) {
	getRuneWeight := collation.Sorter()
	var lBuf, rBuf []byte
	lDone, rDone := false, false

	pullFrom := func() error {
		for (!lDone && !utf8.FullRune(lBuf)) || (!rDone && !utf8.FullRune(rBuf)) {
			lChunk, rChunk, err := d.Next(ctx)
			if err == io.EOF {
				lDone = true
				rDone = true
				return nil
			}
			if err != nil {
				return err
			}
			if len(lChunk) > 0 {
				lBuf = append(lBuf, lChunk...)
			}
			if len(rChunk) > 0 {
				rBuf = append(rBuf, rChunk...)
			}
		}
		return nil
	}
	for {
		if err := pullFrom(); err != nil {
			return 0, err
		}
		switch {
		case len(lBuf) == 0 && len(rBuf) == 0:
			return 0, nil
		case len(lBuf) == 0:
			return -1, nil
		case len(rBuf) == 0:
			return 1, nil
		}
		lr, lread := utf8.DecodeRune(lBuf)
		rr, rread := utf8.DecodeRune(rBuf)
		lErr := lr == utf8.RuneError && lread == 1
		rErr := rr == utf8.RuneError && rread == 1
		if lErr || rErr {
			if lErr && !rErr {
				return 1, nil
			}
			if !lErr && rErr {
				return -1, nil
			}
			return 0, nil
		}
		if lr != rr {
			lw := getRuneWeight(lr)
			rw := getRuneWeight(rr)
			if lw < rw {
				return -1, nil
			}
			if lw > rw {
				return 1, nil
			}
		}
		lBuf = lBuf[lread:]
		rBuf = rBuf[rread:]
	}
}

// chunkDiffer is an interface to diff chunk-encoded values in a ValueStore.
type chunkDiffer interface {
	// Next returns the bytes of the next chunks that differ between left and right. io.EOF signals
	// the end of the diff.
	Next(ctx context.Context) (left, right []byte, err error)
}
