// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/sizecache"
)

// ValueReader is an interface that knows how to read Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value reading.
type ValueReader interface {
	ReadValue(h hash.Hash) Value
}

// ValueWriter is an interface that knows how to write Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value writing.
type ValueWriter interface {
	WriteValue(v Value) Ref
}

// ValueReadWriter is an interface that knows how to read and write Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
	opCache() opCache
}

// ValueStore provides methods to read and write Noms Values to a BatchStore. It validates Values as they are written, but does not guarantee that these Values are persisted to the BatchStore until a subsequent Flush. or Close.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
// - all Refs in v point to a Value that can be read from this ValueStore
// - all Refs in v point to a Value of the correct Type
type ValueStore struct {
	bs         BatchStore
	cache      map[hash.Hash]chunkCacheEntry
	mu         *sync.Mutex
	valueCache *sizecache.SizeCache
	opcStore   opCacheStore
	once       sync.Once
}

const defaultValueCacheSize = 1 << 25 // 32MB

type chunkCacheEntry interface {
	Present() bool
	Hint() hash.Hash
	Type() *Type
}

// NewTestValueStore creates a simple struct that satisfies ValueReadWriter and is backed by a chunks.TestStore.
func NewTestValueStore() *ValueStore {
	return newLocalValueStore(chunks.NewTestStore())
}

func newLocalValueStore(cs chunks.ChunkStore) *ValueStore {
	return NewValueStore(NewBatchStoreAdaptor(cs))
}

// NewValueStore returns a ValueStore instance that owns the provided BatchStore and manages its lifetime. Calling Close on the returned ValueStore will Close bs.
func NewValueStore(bs BatchStore) *ValueStore {
	return NewValueStoreWithCache(bs, defaultValueCacheSize)
}

func NewValueStoreWithCache(bs BatchStore, cacheSize uint64) *ValueStore {
	return &ValueStore{bs, map[hash.Hash]chunkCacheEntry{}, &sync.Mutex{}, sizecache.New(cacheSize), nil, sync.Once{}}
}

func (lvs *ValueStore) BatchStore() BatchStore {
	return lvs.bs
}

// ReadValue reads and decodes a value from lvs. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (lvs *ValueStore) ReadValue(r hash.Hash) Value {
	if v, ok := lvs.valueCache.Get(r); ok {
		if v == nil {
			return nil
		}
		return v.(Value)
	}
	chunk := lvs.bs.Get(r)
	if chunk.IsEmpty() {
		lvs.valueCache.Add(r, 0, nil)
		return nil
	}
	v := DecodeValue(chunk, lvs)
	lvs.valueCache.Add(r, uint64(len(chunk.Data())), v)

	var entry chunkCacheEntry = absentChunk{}
	if v != nil {
		lvs.cacheChunks(v, r)
		// r is trivially a hint for v, so consider putting that in the cache. If we got to v by reading some higher-level chunk, this entry gets dropped on the floor because r already has a hint in the cache. If we later read some other chunk that references v, cacheChunks will overwrite this with a hint pointing to that chunk.
		// If we don't do this, top-level Values that get read but not written -- such as the existing Head of a Database upon a Commit -- can be erroneously left out during a pull.
		entry = hintedChunk{v.Type(), r}
	}
	if cur := lvs.check(r); cur == nil || cur.Hint().IsEmpty() {
		lvs.set(r, entry)
	}
	return v
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns an appropriately-typed types.Ref. v is not guaranteed to be actually written until after Flush().
func (lvs *ValueStore) WriteValue(v Value) Ref {
	d.PanicIfFalse(v != nil)
	// Encoding v causes any child chunks, e.g. internal nodes if v is a meta sequence, to get written. That needs to happen before we try to validate v.
	c := EncodeValue(v, lvs)
	d.PanicIfTrue(c.IsEmpty())
	hash := c.Hash()
	height := maxChunkHeight(v) + 1
	r := constructRef(MakeRefType(v.Type()), hash, height)
	if lvs.isPresent(hash) {
		return r
	}
	hints := lvs.chunkHintsFromCache(v)
	lvs.bs.SchedulePut(c, height, hints)
	lvs.set(hash, (*presentChunk)(v.Type()))
	return r
}

func (lvs *ValueStore) Flush() {
	lvs.bs.Flush()
}

// Close closes the underlying BatchStore
func (lvs *ValueStore) Close() error {
	lvs.Flush()
	if lvs.opcStore != nil {
		err := lvs.opcStore.destroy()
		d.Chk.NoError(err, "Attempt to clean up opCacheStore failed, error: %s\n", err)
		lvs.opcStore = nil
	}
	return lvs.bs.Close()
}

// cacheChunks looks at the Chunks reachable from v and, for each one checks if there's a hint in the cache. If there isn't, or if the hint is a self-reference, the chunk gets r set as its new hint.
func (lvs *ValueStore) cacheChunks(v Value, r hash.Hash) {
	for _, reachable := range v.Chunks() {
		hash := reachable.TargetHash()
		if cur := lvs.check(hash); cur == nil || cur.Hint().IsEmpty() || cur.Hint() == hash {
			lvs.set(hash, hintedChunk{getTargetType(reachable), r})
		}
	}
}

func (lvs *ValueStore) isPresent(r hash.Hash) (present bool) {
	if entry := lvs.check(r); entry != nil && entry.Present() {
		present = true
	}
	return
}

func (lvs *ValueStore) check(r hash.Hash) chunkCacheEntry {
	lvs.mu.Lock()
	defer lvs.mu.Unlock()
	return lvs.cache[r]
}

func (lvs *ValueStore) set(r hash.Hash, entry chunkCacheEntry) {
	lvs.mu.Lock()
	defer lvs.mu.Unlock()
	lvs.cache[r] = entry
}

func (lvs *ValueStore) checkAndSet(r hash.Hash, entry chunkCacheEntry) {
	if cur := lvs.check(r); cur == nil || cur.Hint().IsEmpty() {
		lvs.set(r, entry)
	}
}

func (lvs *ValueStore) chunkHintsFromCache(v Value) Hints {
	return lvs.checkChunksInCache(v, false)
}

func (lvs *ValueStore) ensureChunksInCache(v Value) {
	lvs.checkChunksInCache(v, true)
}

func (lvs *ValueStore) opCache() opCache {
	lvs.once.Do(func() {
		lvs.opcStore = newLdbOpCacheStore(lvs)
	})
	return lvs.opcStore.opCache()
}

func (lvs *ValueStore) checkChunksInCache(v Value, readValues bool) Hints {
	hints := map[hash.Hash]struct{}{}
	for _, reachable := range v.Chunks() {
		// First, check the type cache to see if reachable is already known to be valid.
		targetHash := reachable.TargetHash()
		entry := lvs.check(targetHash)

		// If it's not already in the cache, attempt to read the value directly, which will put it and its chunks into the cache.
		if entry == nil || !entry.Present() {
			var reachableV Value
			if readValues {
				// TODO: log or report that we needed to ReadValue here BUG 1762
				reachableV = lvs.ReadValue(targetHash)
				entry = lvs.check(targetHash)
			}
			if reachableV == nil {
				d.Chk.Fail("Attempted to write Value containing Ref to non-existent object.", "%s\n, contains ref %s, which points to a non-existent Value.", v.Hash(), reachable.TargetHash())
			}
		}
		if hint := entry.Hint(); !hint.IsEmpty() {
			hints[hint] = struct{}{}
		}

		targetType := getTargetType(reachable)
		d.PanicIfTrue(!entry.Type().Equals(targetType), "Value to write contains ref %s, which points to a value of a different type: %+v != %+v", reachable.TargetHash(), entry.Type(), targetType)
	}
	return hints
}

func getTargetType(refBase Ref) *Type {
	refType := refBase.Type()
	d.PanicIfFalse(RefKind == refType.Kind())
	return refType.Desc.(CompoundDesc).ElemTypes[0]
}

type hintedChunk struct {
	t    *Type
	hint hash.Hash
}

func (h hintedChunk) Present() bool {
	return true
}

func (h hintedChunk) Hint() (r hash.Hash) {
	return h.hint
}

func (h hintedChunk) Type() *Type {
	return h.t
}

type presentChunk Type

func (p *presentChunk) Present() bool {
	return true
}

func (p *presentChunk) Hint() (h hash.Hash) {
	return
}

func (p *presentChunk) Type() *Type {
	return (*Type)(p)
}

type absentChunk struct{}

func (a absentChunk) Present() bool {
	return false
}

func (a absentChunk) Hint() (r hash.Hash) {
	return
}

func (a absentChunk) Type() *Type {
	panic("Not reached. Should never call Type() on an absentChunk.")
}
