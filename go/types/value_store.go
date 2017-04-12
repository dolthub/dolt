// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"os"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/sizecache"
)

// ValueReader is an interface that knows how to read Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value reading.
type ValueReader interface {
	ReadValue(h hash.Hash) Value
	ReadManyValues(hashes hash.HashSet, foundValues chan<- Value)
}

// ValueWriter is an interface that knows how to write Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value writing.
type ValueWriter interface {
	WriteValue(v Value) Ref
}

// ValueReadWriter is an interface that knows how to read and write Noms
// Values, e.g. datas/Database. Required to avoid import cycle between this
// package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
	opCache() opCache
}

// ValueStore provides methods to read and write Noms Values to a BatchStore.
// It minimally validates Values as they're written, but does not guarantee
// that these Values are persisted through the BatchStore until a subsequent
// Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
type ValueStore struct {
	bs                   BatchStore
	bufferMu             sync.RWMutex
	bufferedChunks       map[hash.Hash]chunks.Chunk
	bufferedChunksMax    uint64
	bufferedChunkSize    uint64
	withBufferedChildren map[hash.Hash]uint64 // chunk Hash -> ref height
	valueCache           *sizecache.SizeCache
	opcStore             opCacheStore
	once                 sync.Once
}

const (
	defaultValueCacheSize = 1 << 25 // 32MB
	defaultPendingPutMax  = 1 << 28 // 256MB
)

// NewTestValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore.
func NewTestValueStore() *ValueStore {
	return newLocalValueStore(chunks.NewTestStore())
}

func newLocalValueStore(cs chunks.ChunkStore) *ValueStore {
	return NewValueStore(NewBatchStoreAdaptor(cs))
}

// NewValueStore returns a ValueStore instance that owns the provided
// BatchStore and manages its lifetime. Calling Close on the returned
// ValueStore will Close bs.
func NewValueStore(bs BatchStore) *ValueStore {
	return NewValueStoreWithCache(bs, defaultValueCacheSize)
}

func NewValueStoreWithCache(bs BatchStore, cacheSize uint64) *ValueStore {
	return newValueStoreWithCacheAndPending(bs, cacheSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(bs BatchStore, cacheSize, pendingMax uint64) *ValueStore {
	return &ValueStore{
		bs: bs,

		bufferMu:             sync.RWMutex{},
		bufferedChunks:       map[hash.Hash]chunks.Chunk{},
		bufferedChunksMax:    pendingMax,
		withBufferedChildren: map[hash.Hash]uint64{},

		valueCache: sizecache.New(cacheSize),
		once:       sync.Once{},
	}
}

func (lvs *ValueStore) BatchStore() BatchStore {
	return lvs.bs
}

// ReadValue reads and decodes a value from lvs. It is not considered an error
// for the requested chunk to be empty; in this case, the function simply
// returns nil.
func (lvs *ValueStore) ReadValue(h hash.Hash) Value {
	if v, ok := lvs.valueCache.Get(h); ok {
		if v == nil {
			return nil
		}
		return v.(Value)
	}

	chunk := func() chunks.Chunk {
		lvs.bufferMu.RLock()
		defer lvs.bufferMu.RUnlock()
		if pending, ok := lvs.bufferedChunks[h]; ok {
			return pending
		}
		return chunks.EmptyChunk
	}()
	if chunk.IsEmpty() {
		chunk = lvs.bs.Get(h)
	}
	if chunk.IsEmpty() {
		lvs.valueCache.Add(h, 0, nil)
		return nil
	}

	v := DecodeValue(chunk, lvs)
	lvs.valueCache.Add(h, uint64(len(chunk.Data())), v)
	return v
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs. On
// return, |foundValues| will have been fully sent all Values which have been
// found. Any non-present Values will silently be ignored.
func (lvs *ValueStore) ReadManyValues(hashes hash.HashSet, foundValues chan<- Value) {
	decode := func(h hash.Hash, chunk *chunks.Chunk, toPending bool) Value {
		v := DecodeValue(*chunk, lvs)
		lvs.valueCache.Add(h, uint64(len(chunk.Data())), v)
		return v
	}

	// First, see which hashes can be found in either the Value cache or bufferedChunks. Put the rest into a new HashSet to be requested en masse from the BatchStore.
	remaining := hash.HashSet{}
	for h := range hashes {
		if v, ok := lvs.valueCache.Get(h); ok {
			if v != nil {
				foundValues <- v.(Value)
			}
			continue
		}

		chunk := func() chunks.Chunk {
			lvs.bufferMu.RLock()
			defer lvs.bufferMu.RUnlock()
			if pending, ok := lvs.bufferedChunks[h]; ok {
				return pending
			}
			return chunks.EmptyChunk
		}()
		if !chunk.IsEmpty() {
			foundValues <- decode(h, &chunk, true)
			continue
		}

		remaining.Insert(h)
	}

	if len(remaining) == 0 {
		return
	}

	// Request remaining hashes from BatchStore, processing the found chunks as they come in.
	foundChunks := make(chan *chunks.Chunk, 16)
	foundHashes := hash.HashSet{}

	go func() { lvs.bs.GetMany(remaining, foundChunks); close(foundChunks) }()
	for c := range foundChunks {
		h := c.Hash()
		foundHashes[h] = struct{}{}
		foundValues <- decode(h, c, false)
	}

	for h := range foundHashes {
		remaining.Remove(h) // Avoid concurrent access with the call to GetMany above
	}

	// Any remaining hashes weren't found in the BatchStore should be recorded as not present.
	for h := range remaining {
		lvs.valueCache.Add(h, 0, nil)
	}
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns
// an appropriately-typed types.Ref. v is not guaranteed to be actually
// written until after Flush().
func (lvs *ValueStore) WriteValue(v Value) Ref {
	d.PanicIfFalse(v != nil)
	// Encoding v causes any child chunks, e.g. internal nodes if v is a meta sequence, to get written. That needs to happen before we try to validate v.
	c := EncodeValue(v, lvs)
	d.PanicIfTrue(c.IsEmpty())
	h := c.Hash()
	height := maxChunkHeight(v) + 1
	r := constructRef(h, TypeOf(v), height)
	if v, ok := lvs.valueCache.Get(h); ok && v != nil {
		return r
	}

	lvs.bufferChunk(v, c, height)
	lvs.valueCache.Drop(h) // valueCache may have an entry saying h is not present. Clear that.
	return r
}

// bufferChunk enqueues c (which is the serialization of v) within this
// ValueStore. Buffered chunks are flushed progressively to the underlying
// BatchStore in a way which attempts to locate children and grandchildren
// sequentially together. The following invariants are retained:
//
// 1. For any given chunk currently in the buffer, only direct children of the
//    chunk may also be presently buffered (any grandchildren will have been
//    flushed).
// 2. The total data occupied by buffered chunks does not exceed
//    lvs.bufferedChunksMax
func (lvs *ValueStore) bufferChunk(v Value, c chunks.Chunk, height uint64) {
	lvs.bufferMu.Lock()
	defer lvs.bufferMu.Unlock()
	h := c.Hash()
	d.PanicIfTrue(height == 0)
	lvs.bufferedChunks[h] = c
	lvs.bufferedChunkSize += uint64(len(c.Data()))

	putChildren := func(parent hash.Hash) (dataPut int) {
		pending, isBuffered := lvs.bufferedChunks[parent]
		if !isBuffered {
			fmt.Fprintf(os.Stderr, "While buffering chunk for %s, encountered %s which has buffered children, but is not buffered itself.\n", h, parent)
			return
		}
		pv := DecodeValue(pending, lvs)
		pv.WalkRefs(func(grandchildRef Ref) {
			gch := grandchildRef.TargetHash()
			if pending, present := lvs.bufferedChunks[gch]; present {
				lvs.bs.SchedulePut(pending)
				dataPut += len(pending.Data())
				delete(lvs.bufferedChunks, gch)
			}
		})
		delete(lvs.withBufferedChildren, parent)
		return
	}

	// Enforce invariant (1)
	if height > 1 {
		v.WalkRefs(func(childRef Ref) {
			childHash := childRef.TargetHash()
			if _, isBuffered := lvs.bufferedChunks[childHash]; isBuffered {
				lvs.withBufferedChildren[h] = height
			}
			if _, hasBufferedChildren := lvs.withBufferedChildren[childHash]; hasBufferedChildren {
				lvs.bufferedChunkSize -= uint64(putChildren(childHash))
			}
		})
	}

	// Enforce invariant (2)
	for lvs.bufferedChunkSize > lvs.bufferedChunksMax {
		var tallest hash.Hash
		var height uint64 = 0
		for parent, ht := range lvs.withBufferedChildren {
			if ht > height {
				tallest = parent
				height = ht
			}
		}
		if height == 0 { // This can happen if there are no pending parents
			var chunk chunks.Chunk
			for tallest, chunk = range lvs.bufferedChunks {
				// Any pendingPut is as good as another in this case, so take the first one
				break
			}
			lvs.bs.SchedulePut(chunk)
			lvs.bufferedChunkSize -= uint64(len(chunk.Data()))
			delete(lvs.bufferedChunks, tallest)
			continue
		}

		lvs.bufferedChunkSize -= uint64(putChildren(tallest))
	}
}

func (lvs *ValueStore) Flush(root hash.Hash) {
	func() {
		lvs.bufferMu.Lock()
		defer lvs.bufferMu.Unlock()

		pending, present := lvs.bufferedChunks[root]
		if !present {
			return
		}

		v := DecodeValue(pending, lvs)
		put := func(h hash.Hash, chunk chunks.Chunk) uint64 {
			lvs.bs.SchedulePut(chunk)
			delete(lvs.bufferedChunks, h)
			return uint64(len(chunk.Data()))
		}
		v.WalkRefs(func(reachable Ref) {
			if pending, present := lvs.bufferedChunks[reachable.TargetHash()]; present {
				lvs.bufferedChunkSize -= put(reachable.TargetHash(), pending)
			}
		})
		delete(lvs.withBufferedChildren, root) // If not present, this is idempotent
		lvs.bufferedChunkSize -= put(root, pending)
	}()
	lvs.bs.Flush()
}

// Close closes the underlying BatchStore
func (lvs *ValueStore) Close() error {
	if lvs.opcStore != nil {
		err := lvs.opcStore.destroy()
		d.Chk.NoError(err, "Attempt to clean up opCacheStore failed, error: %s\n", err)
		lvs.opcStore = nil
	}
	return lvs.bs.Close()
}

func (lvs *ValueStore) opCache() opCache {
	lvs.once.Do(func() {
		lvs.opcStore = newLdbOpCacheStore(lvs)
	})
	return lvs.opcStore.opCache()
}

func getTargetType(refBase Ref) *Type {
	refType := TypeOf(refBase)
	d.PanicIfFalse(RefKind == refType.TargetKind())
	return refType.Desc.(CompoundDesc).ElemTypes[0]
}
