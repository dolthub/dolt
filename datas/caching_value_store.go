package datas

import (
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type cachingValueStore struct {
	hcs   hintedChunkStore
	cache map[ref.Ref]chunkCacheEntry
	mu    *sync.Mutex
}

type chunkCacheEntry interface {
	Present() bool
	Hint() ref.Ref
	Type() types.Type
}

func newCachingValueStore(hcs hintedChunkStore) cachingValueStore {
	return cachingValueStore{hcs, map[ref.Ref]chunkCacheEntry{}, &sync.Mutex{}}
}

// WriteValue takes a Value, schedules it to be written it to cvs, and returns v.Ref(). v is not guaranteed to be actually written until after a successful Commit().
func (cvs *cachingValueStore) WriteValue(v types.Value) (r types.RefBase) {
	if v == nil {
		return
	}

	targetRef := v.Ref()
	r = types.PrivateRefFromType(targetRef, types.MakeRefType(v.Type()))
	if cvs.isPresent(targetRef) {
		return
	}

	// Encoding v causes any child chunks, e.g. internal nodes if v is a meta sequence, to get written. That needs to happen before we try to validate v.
	c := types.EncodeValue(v, cvs)

	hints := cvs.checkChunksInCache(v)
	cvs.set(targetRef, presentChunk(v.Type()))
	cvs.hcs.Put(c, hints)

	return
}

// ReadValue reads and decodes a value from cvs. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (cvs *cachingValueStore) ReadValue(r ref.Ref) types.Value {
	v := types.DecodeChunk(cvs.hcs.Get(r), cvs)

	var entry chunkCacheEntry = absentChunk{}
	if v != nil {
		entry = presentChunk(v.Type())
		cvs.cacheChunks(v, r)
	}
	if cur := cvs.check(r); cur == nil || cur.Hint().IsEmpty() {
		cvs.set(r, entry)
	}
	return v
}

func (cvs *cachingValueStore) cacheChunks(v types.Value, r ref.Ref) {
	for _, reachable := range v.Chunks() {
		hash := reachable.TargetRef()
		if cur := cvs.check(hash); cur == nil || cur.Hint().IsEmpty() {
			cvs.set(hash, hintedChunk{getTargetType(reachable), r})
			// Code-genned Packages are side-loaded when reading Values for performance reasons. This means that they won't pass through the ReadValue() codepath above, which means that they won't have their Chunks added to the cache. So, if reachable is a RefOfPackage, go look the package up in the types.PackageRegistry and recursively add its Chunks to the cache.
			if _, ok := reachable.(types.RefOfPackage); ok {
				if p := types.LookupPackage(hash); p != nil {
					cvs.cacheChunks(p, hash)
				}
			}
		}
	}
}

func (cvs *cachingValueStore) isPresent(r ref.Ref) (present bool) {
	if entry := cvs.check(r); entry != nil && entry.Present() {
		present = true
	}
	return
}

func (cvs *cachingValueStore) check(r ref.Ref) chunkCacheEntry {
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	return cvs.cache[r]
}

func (cvs *cachingValueStore) set(r ref.Ref, entry chunkCacheEntry) {
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	cvs.cache[r] = entry
}

func (cvs *cachingValueStore) checkAndSet(r ref.Ref, entry chunkCacheEntry) {
	if cur := cvs.check(r); cur == nil || cur.Hint().IsEmpty() {
		cvs.set(r, entry)
	}
}

func (cvs *cachingValueStore) checkChunksInCache(v types.Value) map[ref.Ref]struct{} {
	hints := map[ref.Ref]struct{}{}
	for _, reachable := range v.Chunks() {
		entry := cvs.check(reachable.TargetRef())
		d.Exp.True(entry != nil && entry.Present(), "Value to write -- Type %s -- contains ref %s, which points to a non-existent Value.", v.Type().Describe(), reachable.TargetRef())
		if hint := entry.Hint(); !hint.IsEmpty() {
			hints[hint] = struct{}{}
		}

		// BUG 1121
		// It's possible that entry.Type() will be simply 'Value', but that 'reachable' is actually a properly-typed object -- that is, a Ref to some specific Type. The Exp below would fail, though it's possible that the Type is actually correct. We wouldn't be able to verify without reading it, though, so we'll dig into this later.
		targetType := getTargetType(reachable)
		if targetType.Equals(types.MakePrimitiveType(types.ValueKind)) {
			continue
		}
		d.Exp.True(entry.Type().Equals(targetType), "Value to write contains ref %s, which points to a value of a different type: %+v != %+v", reachable.TargetRef(), entry.Type(), targetType)
	}
	return hints
}

func getTargetType(refBase types.RefBase) types.Type {
	refType := refBase.Type()
	d.Chk.Equal(types.RefKind, refType.Kind())
	return refType.Desc.(types.CompoundDesc).ElemTypes[0]
}

type presentChunk types.Type

func (p presentChunk) Present() bool {
	return true
}

func (p presentChunk) Hint() (r ref.Ref) {
	return
}

func (p presentChunk) Type() types.Type {
	return types.Type(p)
}

type hintedChunk struct {
	t    types.Type
	hint ref.Ref
}

func (h hintedChunk) Present() bool {
	return true
}

func (h hintedChunk) Hint() (r ref.Ref) {
	return h.hint
}

func (h hintedChunk) Type() types.Type {
	return h.t
}

type absentChunk struct{}

func (a absentChunk) Present() bool {
	return false
}

func (a absentChunk) Hint() (r ref.Ref) {
	return
}

func (a absentChunk) Type() types.Type {
	panic("Not reached. Should never call Type() on an absentChunk.")
}
