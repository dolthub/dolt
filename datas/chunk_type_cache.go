package datas

import (
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type chunkTypeCache struct {
	cache map[ref.Ref]chunkCacheEntry
	mu    *sync.Mutex
}

type chunkCacheEntry interface {
	Present() bool
	Hint() ref.Ref
	Type() types.Type
}

func newChunkTypeCache() *chunkTypeCache {
	return &chunkTypeCache{map[ref.Ref]chunkCacheEntry{}, &sync.Mutex{}}
}

func (c *chunkTypeCache) isPresent(r ref.Ref) (present bool) {
	if entry := c.check(r); entry != nil && entry.Present() {
		present = true
	}
	return
}

func (c *chunkTypeCache) check(r ref.Ref) chunkCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache[r]
}

func (c *chunkTypeCache) set(r ref.Ref, entry chunkCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[r] = entry
}

func (c *chunkTypeCache) checkAndSet(r ref.Ref, entry chunkCacheEntry) {
	if cur := c.check(r); cur == nil || cur.Hint().IsEmpty() {
		c.set(r, entry)
	}
}

func (c *chunkTypeCache) checkChunksInCache(v types.Value) map[ref.Ref]struct{} {
	hints := map[ref.Ref]struct{}{}
	for _, reachable := range v.Chunks() {
		entry := c.check(reachable.TargetRef())
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
