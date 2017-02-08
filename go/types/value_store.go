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
// It validates Values as they are written, but does not guarantee that these
// Values are persisted through the BatchStore until a subsequent Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
// - all Refs in v point to a Value that can be read from this ValueStore
// - all Refs in v point to a Value of the correct Type
type ValueStore struct {
	bs             BatchStore
	cacheMu        sync.RWMutex
	hintCache      map[hash.Hash]chunkCacheEntry
	pendingHints   map[hash.Hash]chunkCacheEntry
	pendingMu      sync.RWMutex
	pendingPuts    map[hash.Hash]pendingChunk
	pendingPutMax  uint64
	pendingPutSize uint64
	pendingParents map[hash.Hash]uint64 // chunk Hash -> ref height
	valueCache     *sizecache.SizeCache
	opcStore       opCacheStore
	once           sync.Once
}

const (
	defaultValueCacheSize = 1 << 25 // 32MB
	defaultPendingPutMax  = 1 << 28 // 256MB
)

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
	return newValueStoreWithCacheAndPending(bs, cacheSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(bs BatchStore, cacheSize, pendingMax uint64) *ValueStore {
	return &ValueStore{
		bs:             bs,
		cacheMu:        sync.RWMutex{},
		hintCache:      map[hash.Hash]chunkCacheEntry{},
		pendingHints:   map[hash.Hash]chunkCacheEntry{},
		pendingMu:      sync.RWMutex{},
		pendingPuts:    map[hash.Hash]pendingChunk{},
		pendingPutMax:  pendingMax,
		pendingParents: map[hash.Hash]uint64{},

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

	stillPending := false
	chunk := func() chunks.Chunk {
		lvs.pendingMu.RLock()
		defer lvs.pendingMu.RUnlock()
		if pc, ok := lvs.pendingPuts[h]; ok {
			stillPending = true
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
	lvs.setHintsForReadValue(v, h, stillPending)
	return v
}

func (lvs *ValueStore) setHintsForReadValue(v Value, h hash.Hash, toPending bool) {
	var entry chunkCacheEntry = absentChunk{}
	if v != nil {
		lvs.setHintsForReachable(v, h, toPending)
		// h is trivially a hint for v, so consider putting that in the hintCache. If we got to v by reading some higher-level chunk, this entry gets dropped on the floor because h already has a hint in the hintCache. If we later read some other chunk that references v, setHintsForReachable will overwrite this with a hint pointing to that chunk.
		// If we don't do this, top-level Values that get read but not written -- such as the existing Head of a Database upon a Commit -- can be erroneously left out during a pull.
		entry = hintedChunk{v.Type(), h}
	}
	if cur := lvs.check(h); cur == nil || cur.Hint().IsEmpty() {
		lvs.set(h, entry, toPending)
	}
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs. On
// return, |foundValues| will have been fully sent all Values which have been
// found. Any non-present Values will silently be ignored.
func (lvs *ValueStore) ReadManyValues(hashes hash.HashSet, foundValues chan<- Value) {
	decode := func(h hash.Hash, chunk *chunks.Chunk, toPending bool) Value {
		v := DecodeValue(*chunk, lvs)
		lvs.valueCache.Add(h, uint64(len(chunk.Data())), v)
		lvs.setHintsForReadValue(v, h, toPending)
		return v
	}

	// First, see which hashes can be found in either the Value cache or pendingPuts. Put the rest into a new HashSet to be requested en masse from the BatchStore.
	remaining := hash.HashSet{}
	for h := range hashes {
		if v, ok := lvs.valueCache.Get(h); ok {
			if v != nil {
				foundValues <- v.(Value)
			}
			continue
		}

		chunk := func() chunks.Chunk {
			lvs.pendingMu.RLock()
			defer lvs.pendingMu.RUnlock()
			if pc, ok := lvs.pendingPuts[h]; ok {
				return pc.c
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
	r := constructRef(MakeRefType(v.Type()), h, height)
	if lvs.isPresent(h) {
		return r
	}

	// TODO: It _really_ feels like there should be some refactoring that allows us to only have to walk the refs of |v| once, but I'm hesitant to undertake that refactor right now.
	hints := lvs.chunkHintsFromCache(v)
	lvs.bufferChunk(v, c, height, hints)

	lvs.setHintsForReachable(v, h, true)
	lvs.set(h, (*presentChunk)(v.Type()), false)
	lvs.valueCache.Drop(h)
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
//    lvs.pendingPutMax
func (lvs *ValueStore) bufferChunk(v Value, c chunks.Chunk, height uint64, hints Hints) {
	lvs.pendingMu.Lock()
	defer lvs.pendingMu.Unlock()
	h := c.Hash()
	d.Chk.NotZero(height)
	lvs.pendingPuts[h] = pendingChunk{c, height, hints}
	lvs.pendingPutSize += uint64(len(c.Data()))

	putChildren := func(parent hash.Hash) (dataPut int) {
		pc, present := lvs.pendingPuts[parent]
		d.Chk.True(present)
		v := DecodeValue(pc.c, lvs)
		v.WalkRefs(func(grandchildRef Ref) {
			if pc, present := lvs.pendingPuts[grandchildRef.TargetHash()]; present {
				lvs.bs.SchedulePut(pc.c, pc.height, pc.hints)
				dataPut += len(pc.c.Data())
				delete(lvs.pendingPuts, grandchildRef.TargetHash())
			}
		})
		return
	}

	// Enforce invariant (1)
	if height > 1 {
		v.WalkRefs(func(childRef Ref) {
			childHash := childRef.TargetHash()
			if _, present := lvs.pendingPuts[childHash]; present {
				lvs.pendingParents[h] = height
			} else {
				// Shouldn't be able to be in pendingParents without being in pendingPuts
				_, present := lvs.pendingParents[childHash]
				d.Chk.False(present)
			}

			if _, present := lvs.pendingParents[childHash]; present {
				lvs.pendingPutSize -= uint64(putChildren(childHash))
				delete(lvs.pendingParents, childHash)
			}
		})
	}

	// Enforce invariant (2)
	for lvs.pendingPutSize > lvs.pendingPutMax {
		var tallest hash.Hash
		var height uint64 = 0
		for parent, ht := range lvs.pendingParents {
			if ht > height {
				tallest = parent
				height = ht
			}
		}
		if height == 0 { // This can happen if there are no pending parents
			var pc pendingChunk
			for tallest, pc = range lvs.pendingPuts {
				// Any pendingPut is as good as another in this case, so take the first one
				break
			}
			lvs.bs.SchedulePut(pc.c, pc.height, pc.hints)
			lvs.pendingPutSize -= uint64(len(pc.c.Data()))
			delete(lvs.pendingPuts, tallest)
			continue
		}

		lvs.pendingPutSize -= uint64(putChildren(tallest))
		delete(lvs.pendingParents, tallest)
	}
}

func (lvs *ValueStore) Flush(root hash.Hash) {
	func() {
		lvs.pendingMu.Lock()
		defer lvs.pendingMu.Unlock()

		pc, present := lvs.pendingPuts[root]
		if !present {
			return
		}

		put := func(h hash.Hash, pc pendingChunk) uint64 {
			lvs.bs.SchedulePut(pc.c, pc.height, pc.hints)
			delete(lvs.pendingPuts, h)
			return uint64(len(pc.c.Data()))
		}
		v := DecodeValue(pc.c, lvs)
		v.WalkRefs(func(reachable Ref) {
			if pc, present := lvs.pendingPuts[reachable.TargetHash()]; present {
				lvs.pendingPutSize -= put(reachable.TargetHash(), pc)
			}
		})
		delete(lvs.pendingParents, root) // If not present, this is idempotent
		lvs.pendingPutSize -= put(root, pc)

		// Merge in pending hints
		lvs.cacheMu.Lock()
		defer lvs.cacheMu.Unlock()
		for h, entry := range lvs.pendingHints {
			if _, present := lvs.pendingPuts[h]; !present {
				lvs.hintCache[h] = entry
				delete(lvs.pendingHints, h)
			}
		}
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
		// First, check the hintCache to see if reachable is already known to be valid.
		targetHash := reachable.TargetHash()
		entry := lvs.check(targetHash)

		// If it's not already in the hintCache, attempt to read the value directly, which will put it and its chunks into the hintCache.
		var reachableV Value
		if entry == nil || !entry.Present() {
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
		if entry.Type().Kind() == ValueKind && targetType.Kind() != ValueKind {
			// We've seen targetHash before, but only in a Ref<Value>, and reachable has a more specific type than that. Deref reachable to check the real type on the chunk it points to, and cache the result if everything checks out.
			if reachableV == nil {
				reachableV = lvs.ReadValue(targetHash)
			}
			entry = hintedChunk{reachableV.Type(), entry.Hint()}
			lvs.set(targetHash, entry, false)
		}
		// At this point, entry should have the most specific type info possible. Unless it matches targetType, or targetType is 'Value', bail.
		if !(targetType.Kind() == ValueKind || entry.Type().Equals(targetType)) {
			d.Panic("Value to write contains ref %s, which points to a value of a different type: %s != %s", reachable.TargetHash(), targetType.Describe(), entry.Type().Describe())
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
