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

// ValueReader is an interface that knows how to read Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value reading.
type ValueReader interface {
	ReadValue(h hash.Hash) Value
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
// It validates Values as they are written, but does not guarantee that these
// Values are persisted through the BatchStore until a subsequent Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
// - all Refs in v point to a Value that can be read from this ValueStore
// - all Refs in v point to a Value of the correct Type
type ValueStore struct {
	bs           BatchStore
	cacheMu      sync.RWMutex
	hintCache    map[hash.Hash]chunkCacheEntry
	pendingHints map[hash.Hash]chunkCacheEntry
	pendingMu    sync.RWMutex
	pendingPuts  map[hash.Hash]pendingChunk
	valueCache   *sizecache.SizeCache
	opcStore     opCacheStore
	once         sync.Once
}

const defaultValueCacheSize = 1 << 25 // 32MB

type chunkCacheEntry interface {
	Present() bool
	Hint() hash.Hash
	Type() *Type
}

type pendingChunk struct {
	c      chunks.Chunk
	height uint64
	hints  Hints
}

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
	return &ValueStore{
		bs:           bs,
		cacheMu:      sync.RWMutex{},
		hintCache:    map[hash.Hash]chunkCacheEntry{},
		pendingHints: map[hash.Hash]chunkCacheEntry{},
		pendingMu:    sync.RWMutex{},
		pendingPuts:  map[hash.Hash]pendingChunk{},
		valueCache:   sizecache.New(cacheSize),
		once:         sync.Once{},
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
		lvs.pendingMu.RLock()
		defer lvs.pendingMu.RUnlock()
		if pc, ok := lvs.pendingPuts[h]; ok {
			return pc.c
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
	lvs.setHintsForReadValue(v, h)
	return v
}

func (lvs *ValueStore) setHintsForReadValue(v Value, h hash.Hash) {
	var entry chunkCacheEntry = absentChunk{}
	if v != nil {
		lvs.setHintsForReachable(v, h, false)
		// h is trivially a hint for v, so consider putting that in the hintCache. If we got to v by reading some higher-level chunk, this entry gets dropped on the floor because h already has a hint in the hintCache. If we later read some other chunk that references v, setHintsForReachable will overwrite this with a hint pointing to that chunk.
		// If we don't do this, top-level Values that get read but not written -- such as the existing Head of a Database upon a Commit -- can be erroneously left out during a pull.
		entry = hintedChunk{v.Type(), h}
	}
	if cur := lvs.check(h); cur == nil || cur.Hint().IsEmpty() {
		lvs.set(h, entry, false)
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
	r := constructRef(MakeRefType(v.Type()), h, height)
	if lvs.isPresent(h) {
		return r
	}

	// TODO: It _really_ feels like there should be some refactoring that allows us to only have to walk the refs of |v| once, but I'm hesitant to undertake that refactor right now.
	hints := lvs.chunkHintsFromCache(v)

	func() {
		lvs.pendingMu.Lock()
		defer lvs.pendingMu.Unlock()
		lvs.pendingPuts[h] = pendingChunk{c, height, hints}
		v.WalkRefs(func(reachable Ref) {
			if pc, present := lvs.pendingPuts[reachable.TargetHash()]; present {
				lvs.bs.SchedulePut(pc.c, pc.height, pc.hints)
				delete(lvs.pendingPuts, reachable.TargetHash())
			}
		})
	}()

	lvs.setHintsForReachable(v, v.Hash(), true)
	lvs.set(h, (*presentChunk)(v.Type()), false)
	lvs.valueCache.Drop(h)
	return r
}

func (lvs *ValueStore) Flush() {
	func() {
		lvs.pendingMu.Lock()
		defer lvs.pendingMu.Unlock()
		for _, pc := range lvs.pendingPuts {
			lvs.bs.SchedulePut(pc.c, pc.height, pc.hints)
		}
		lvs.pendingPuts = map[hash.Hash]pendingChunk{}
	}()
	lvs.mergePendingHints()
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

// setHintsForReachable looks at the Chunks reachable from v and, for each one checks if there's a hint in the hintCache. If there isn't, or if the hint is a self-reference, the chunk gets r set as its new hint.
func (lvs *ValueStore) setHintsForReachable(v Value, r hash.Hash, toPending bool) {
	v.WalkRefs(func(reachable Ref) {
		hash := reachable.TargetHash()
		if cur := lvs.check(hash); cur == nil || cur.Hint().IsEmpty() || cur.Hint() == hash {
			lvs.set(hash, hintedChunk{getTargetType(reachable), r}, toPending)
		}
	})
}

func (lvs *ValueStore) isPresent(r hash.Hash) (present bool) {
	if entry := lvs.check(r); entry != nil && entry.Present() {
		present = true
	}
	return
}

func (lvs *ValueStore) check(r hash.Hash) chunkCacheEntry {
	lvs.cacheMu.RLock()
	defer lvs.cacheMu.RUnlock()
	return lvs.hintCache[r]
}

func (lvs *ValueStore) set(r hash.Hash, entry chunkCacheEntry, toPending bool) {
	lvs.cacheMu.Lock()
	defer lvs.cacheMu.Unlock()
	if toPending {
		lvs.pendingHints[r] = entry
	} else {
		lvs.hintCache[r] = entry
	}
}

func (lvs *ValueStore) mergePendingHints() {
	lvs.cacheMu.Lock()
	defer lvs.cacheMu.Unlock()

	for h, entry := range lvs.pendingHints {
		lvs.hintCache[h] = entry
	}
	lvs.pendingHints = map[hash.Hash]chunkCacheEntry{}
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
	collectHints := func(reachable Ref) {
		// First, check the  hintCache to see if reachable is already known to be valid.
		targetHash := reachable.TargetHash()
		entry := lvs.check(targetHash)

		// If it's not already in the hintCache, attempt to read the value directly, which will put it and its chunks into the hintCache.
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
		if !entry.Type().Equals(targetType) {
			d.Panic("Value to write contains ref %s, which points to a value of a different type: %+v != %+v", reachable.TargetHash(), entry.Type(), targetType)
		}
	}
	v.WalkRefs(collectHints)
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
