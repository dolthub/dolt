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
	"cmp"
	"context"
	"io"
	"sync"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"

	"github.com/dolthub/dolt/go/gen/fb/serial"
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

	// ChunkFileID returns the file identifier of the chunk at |ref|, or an empty string if it is not in the store.
	ChunkFileID(ctx context.Context, ref hash.Hash) (string, error)

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
	n, key, ok := ns.cache.get(ref)
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
	ns.cache.insert(key, n)

	return n, nil
}

// ReadMany implements NodeStore.
func (ns *nodeStore) ReadMany(ctx context.Context, addrs hash.HashSlice) ([]*Node, error) {
	found := make(map[hash.Hash]*Node)
	gets := hash.HashSet{}
	keys := make(map[hash.Hash]cacheKey)

	for _, r := range addrs {
		n, key, ok := ns.cache.get(r)
		if ok {
			found[r] = n
		} else {
			gets.Insert(r)
			keys[r] = key
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
		if !ok {
			continue
		}
		// Anything not in |keys| came out of the cache already.
		if key, fetched := keys[addr]; fetched {
			ns.cache.insert(key, nodes[i])
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

	// Taken before the Put so that a purge racing with it drops the
	// insert below.
	_, key, _ := ns.cache.get(c.Hash())
	if err := ns.store.Put(ctx, c, getAddrs); err != nil {
		return hash.Hash{}, err
	}
	nd.nodeHash.Store(c.Hash())
	ns.cache.insert(key, nd)
	return c.Hash(), nil
}

// ChunkFileID implements NodeStore.
func (ns *nodeStore) ChunkFileID(ctx context.Context, ref hash.Hash) (string, error) {
	c, err := ns.store.Get(ctx, ref)
	if err != nil {
		return "", err
	}
	if c.IsEmpty() {
		return "", nil
	}
	return serial.GetFileID(c.Data()), nil
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

	var leaves [][]byte
	size := 0
	err = WalkNodes(ctx, n, ns, func(ctx context.Context, n *Node) error {
		if n.IsLeaf() {
			leaves = append(leaves, n.GetValue(0))
			size += len(n.GetValue(0))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result = make([]byte, 0, size)
	for _, l := range leaves {
		result = append(result, l...)
	}
	return result, nil
}

// CompareJsonAdaptiveValues implements val.JsonAdaptiveValueComparator. The work is done by
// IndexedJsonDocument.Compare, which compares two JSON documents chunk-by-chunk and applies
// MySQL's JSON ordering rules.
func (ns *nodeStore) CompareJsonAdaptiveValues(ctx context.Context, l, r val.AdaptiveValue) (int, error) {
	return compareJsonAdaptiveValues(ctx, ns, l, r)
}

// WriteBytesReusing implements val.PriorBytesWriter.
func (ns *nodeStore) WriteBytesReusing(ctx context.Context, b []byte, prior hash.Hash) (hash.Hash, error) {
	return SerializeBytesToAddrReusing(ctx, ns, b, prior)
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
	// TODO: the call to Next() starts at the first differing chunk, then returns every leaf chunk in order from both
	//  sides thereafter. This is correct, but potentially inefficient in the case where there are sparse byte differences
	//  that are collation-equivalent spread through two large documents we are sorting. We could more efficiently
	//  perform the comparison in this case by having Next() and NextDiff() methods for the two traversal cases.
	s := collatedChunkStreams{
		ctx:       ctx,
		differ:    d,
		collation: collation,
		padSpace:  collation.IsUCA() && collation.IsPadSpace(),
	}
	for {
		if err := s.fill(&s.left, 0); err != nil {
			return 0, err
		}
		if err := s.fill(&s.right, 0); err != nil {
			return 0, err
		}
		if len(s.left) > 0 && len(s.right) > 0 {
			lr, lSize := utf8.DecodeRune(s.left)
			rr, rSize := utf8.DecodeRune(s.right)
			if lr == rr {
				s.left, s.right = s.left[lSize:], s.right[rSize:]
				continue
			}
		}
		l, err := s.next(&s.left)
		if err != nil {
			return 0, err
		}
		r, err := s.next(&s.right)
		if err != nil {
			return 0, err
		}
		if l.ok && r.ok && l.w == r.w {
			// Under UCA PAD SPACE, a literal trailing space trims while other
			// space-weight runes (such as NBSP) do not. Check for logical end
			// when one side is a space and the other is a distinct rune.
			if !s.padSpace || l.r == r.r || (l.r != ' ' && r.r != ' ') {
				continue
			}
			lEnded, rEnded, err := s.endedBoth(l, r)
			if err != nil {
				return 0, err
			}
			if !lEnded && !rEnded {
				continue
			}
			if lEnded {
				return -1, nil
			}
			return 1, nil
		}
		return s.order(l, r)
	}
}

// collatedChunkStreams reads the runes of two chunked values in lockstep.
type collatedChunkStreams struct {
	ctx         context.Context
	differ      chunkDiffer
	collation   sql.CollationID
	padSpace    bool
	left, right []byte
	eof         bool
}

// weightedRune holds a decoded rune and its collation weight. Field ok is false
// at end of input.
type weightedRune struct {
	r  rune
	w  int32
	ok bool
}

// next consumes runes from |buf| until one carries a weight.
func (s *collatedChunkStreams) next(buf *[]byte) (weightedRune, error) {
	for {
		if err := s.fill(buf, 0); err != nil {
			return weightedRune{}, err
		}
		if len(*buf) == 0 {
			return weightedRune{}, nil
		}
		// Pass malformed bytes through rather than erroring so stored values
		// with invalid UTF-8 keep a deterministic, transitive tree order that
		// matches in-memory comparisons.
		r, size := utf8.DecodeRune(*buf)
		w, ok := s.weight((*buf)[:size])
		*buf = (*buf)[size:]
		if ok {
			return weightedRune{r: r, w: w, ok: true}, nil
		}
	}
}

// order decides the comparison at the first differing weight or end of input.
func (s *collatedChunkStreams) order(l, r weightedRune) (int, error) {
	lEnded, rEnded, err := s.endedBoth(l, r)
	if err != nil {
		return 0, err
	}
	switch {
	case lEnded && rEnded:
		return 0, nil
	case lEnded:
		return -1, nil
	case rEnded:
		return 1, nil
	default:
		return cmp.Compare(l.w, r.w), nil
	}
}

// endedBoth reports whether the left and right values have logically ended.
func (s *collatedChunkStreams) endedBoth(l, r weightedRune) (bool, bool, error) {
	lEnded, err := s.ended(&s.left, l)
	if err != nil {
		return false, false, err
	}
	rEnded, err := s.ended(&s.right, r)
	if err != nil {
		return false, false, err
	}
	return lEnded, rEnded, nil
}

// ended reports whether |wr| marks the logical end of its value. Input has
// ended when exhausted, or under UCA PAD SPACE when |wr| is a literal space
// followed only by trimmable runes, matching TrimRightSpace in the inline path.
func (s *collatedChunkStreams) ended(buf *[]byte, wr weightedRune) (bool, error) {
	if !wr.ok {
		return true, nil
	}
	if !s.padSpace || wr.r != ' ' {
		return false, nil
	}
	return s.trimmableRemainder(buf)
}

// trimmableRemainder reports whether everything left in |buf|, including
// unpulled chunks, is spaces or ignorable runes, without consuming |buf|.
func (s *collatedChunkStreams) trimmableRemainder(buf *[]byte) (bool, error) {
	for i := 0; ; {
		if err := s.fill(buf, i); err != nil {
			return false, err
		}
		if i == len(*buf) {
			return true, nil
		}
		r, size := utf8.DecodeRune((*buf)[i:])
		if r != ' ' {
			if _, ok := s.weight((*buf)[i : i+size]); ok {
				return false, nil
			}
		}
		i += size
	}
}

// weight returns the weight of the single rune encoded in |b|, or false if
// the collation ignores it.
func (s *collatedChunkStreams) weight(b []byte) (int32, bool) {
	ws := sql.NewWeightScanner(s.collation, encodings.BytesToString(b))
	return ws.Next()
}

// fill pulls chunks until |buf| at |offset| starts with a complete rune or EOF.
func (s *collatedChunkStreams) fill(buf *[]byte, offset int) error {
	for !s.eof && !utf8.FullRune((*buf)[offset:]) {
		if err := s.pull(); err != nil {
			return err
		}
	}
	return nil
}

// pull reads the next differing chunks from the stream into left and right.
func (s *collatedChunkStreams) pull() error {
	l, r, err := s.differ.Next(s.ctx)
	if err == io.EOF {
		s.eof = true
		return nil
	}
	if err != nil {
		return err
	}
	s.left = append(s.left, l...)
	s.right = append(s.right, r...)
	return nil
}

// chunkDiffer is an interface to diff chunk-encoded values in a ValueStore.
type chunkDiffer interface {
	// Next returns the bytes of the next chunks that differ between left and right. io.EOF signals
	// the end of the diff.
	Next(ctx context.Context) (left, right []byte, err error)
}
