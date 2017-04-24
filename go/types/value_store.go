// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
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

// ValueStore provides methods to read and write Noms Values to a ChunkStore.
// It minimally validates Values as they're written, but does not guarantee
// that these Values are persisted through the ChunkStore until a subsequent
// Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
type ValueStore struct {
	cs                   chunks.ChunkStore
	bufferMu             sync.RWMutex
	bufferedChunks       map[hash.Hash]chunks.Chunk
	bufferedChunksMax    uint64
	bufferedChunkSize    uint64
	withBufferedChildren map[hash.Hash]uint64 // chunk Hash -> ref height
	valueCache           *sizecache.SizeCache

	opcOnce  sync.Once
	opcStore opCacheStore

	versOnce sync.Once
}

const (
	defaultValueCacheSize = 1 << 25 // 32MB
	defaultPendingPutMax  = 1 << 28 // 256MB
)

// newTestValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore.
func newTestValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewView())
}

// NewValueStore returns a ValueStore instance that owns the provided
// ChunkStore and manages its lifetime. Calling Close on the returned
// ValueStore will Close() cs.
func NewValueStore(cs chunks.ChunkStore) *ValueStore {
	return newValueStoreWithCacheAndPending(cs, defaultValueCacheSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(cs chunks.ChunkStore, cacheSize, pendingMax uint64) *ValueStore {
	return &ValueStore{
		cs: cs,

		bufferMu:             sync.RWMutex{},
		bufferedChunks:       map[hash.Hash]chunks.Chunk{},
		bufferedChunksMax:    pendingMax,
		withBufferedChildren: map[hash.Hash]uint64{},

		opcOnce:    sync.Once{},
		valueCache: sizecache.New(cacheSize),

		versOnce: sync.Once{},
	}
}

func (lvs *ValueStore) expectVersion() {
	dataVersion := lvs.cs.Version()
	if constants.NomsVersion != dataVersion {
		d.Panic("SDK version %s incompatible with data of version %s", constants.NomsVersion, dataVersion)
	}
}

func (lvs *ValueStore) ChunkStore() chunks.ChunkStore {
	return lvs.cs
}

// ReadValue reads and decodes a value from lvs. It is not considered an error
// for the requested chunk to be empty; in this case, the function simply
// returns nil.
func (lvs *ValueStore) ReadValue(h hash.Hash) Value {
	lvs.versOnce.Do(lvs.expectVersion)
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
		chunk = lvs.cs.Get(h)
	}
	if chunk.IsEmpty() {
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
	lvs.versOnce.Do(lvs.expectVersion)
	decode := func(h hash.Hash, chunk *chunks.Chunk, toPending bool) Value {
		v := DecodeValue(*chunk, lvs)
		lvs.valueCache.Add(h, uint64(len(chunk.Data())), v)
		return v
	}

	// First, see which hashes can be found in either the Value cache or bufferedChunks. Put the rest into a new HashSet to be requested en masse from the ChunkStore.
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

	// Request remaining hashes from ChunkStore, processing the found chunks as they come in.
	foundChunks := make(chan *chunks.Chunk, 16)
	foundHashes := hash.HashSet{}

	go func() { lvs.cs.GetMany(remaining, foundChunks); close(foundChunks) }()
	for c := range foundChunks {
		h := c.Hash()
		foundHashes[h] = struct{}{}
		foundValues <- decode(h, c, false)
	}
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns
// an appropriately-typed types.Ref. v is not guaranteed to be actually
// written until after Flush().
func (lvs *ValueStore) WriteValue(v Value) Ref {
	lvs.versOnce.Do(lvs.expectVersion)
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
	return r
}

// bufferChunk enqueues c (which is the serialization of v) within this
// ValueStore. Buffered chunks are flushed progressively to the underlying
// ChunkStore in a way which attempts to locate children and grandchildren
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
	d.PanicIfTrue(height == 0)
	h := c.Hash()
	if _, present := lvs.bufferedChunks[h]; !present {
		lvs.bufferedChunks[h] = c
		lvs.bufferedChunkSize += uint64(len(c.Data()))
	}

	put := func(h hash.Hash, c chunks.Chunk) {
		lvs.cs.Put(c)
		lvs.bufferedChunkSize -= uint64(len(c.Data()))
		delete(lvs.bufferedChunks, h)
	}

	putChildren := func(parent hash.Hash) {
		pending, isBuffered := lvs.bufferedChunks[parent]
		if !isBuffered {
			return
		}
		pv := DecodeValue(pending, lvs)
		pv.WalkRefs(func(grandchildRef Ref) {
			gch := grandchildRef.TargetHash()
			if pending, present := lvs.bufferedChunks[gch]; present {
				put(gch, pending)
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
				putChildren(childHash)
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
			put(tallest, chunk)
			continue
		}

		putChildren(tallest)
	}
}

// Flush() puts all bufferedChunks into the ChunkStore, with best-effort
// locality. NB: The Chunks will not be made durable unless the caller also
// Commits to the underlying ChunkStore.
func (lvs *ValueStore) Flush() {
	func() {
		lvs.bufferMu.Lock()
		defer lvs.bufferMu.Unlock()

		put := func(h hash.Hash, chunk chunks.Chunk) {
			lvs.cs.Put(chunk)
			delete(lvs.bufferedChunks, h)
			lvs.bufferedChunkSize -= uint64(len(chunk.Data()))
		}

		for parent := range lvs.withBufferedChildren {
			if pending, present := lvs.bufferedChunks[parent]; present {
				v := DecodeValue(pending, lvs)
				v.WalkRefs(func(reachable Ref) {
					if pending, present := lvs.bufferedChunks[reachable.TargetHash()]; present {
						put(reachable.TargetHash(), pending)
					}
				})
				put(parent, pending)
			}
		}
		for _, c := range lvs.bufferedChunks {
			// Can't use put() because it's wrong to delete from a lvs.bufferedChunks while iterating it.
			lvs.cs.Put(c)
			lvs.bufferedChunkSize -= uint64(len(c.Data()))
		}
		d.PanicIfFalse(lvs.bufferedChunkSize == 0)
		lvs.withBufferedChildren = map[hash.Hash]uint64{}
		lvs.bufferedChunks = map[hash.Hash]chunks.Chunk{}
	}()
}

// persist() calls Flush(), but also flushes the ChunkStore to make the Chunks
// durable. If you're using this outside of tests, you're probably holding it
// wrong.
func (lvs *ValueStore) persist() {
	lvs.Flush()
	d.PanicIfFalse(lvs.cs.Commit(lvs.cs.Root(), lvs.cs.Root()))
}

// Close closes the underlying ChunkStore
func (lvs *ValueStore) Close() error {
	if lvs.opcStore != nil {
		err := lvs.opcStore.destroy()
		d.Chk.NoError(err, "Attempt to clean up opCacheStore failed, error: %s\n", err)
		lvs.opcStore = nil
	}
	return lvs.cs.Close()
}

func (lvs *ValueStore) opCache() opCache {
	lvs.opcOnce.Do(func() {
		lvs.opcStore = newLdbOpCacheStore(lvs)
	})
	return lvs.opcStore.opCache()
}

func getTargetType(refBase Ref) *Type {
	refType := TypeOf(refBase)
	d.PanicIfFalse(RefKind == refType.TargetKind())
	return refType.Desc.(CompoundDesc).ElemTypes[0]
}
