package datas

import (
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type cachingValueStore struct {
	*chunkTypeCache
	hcs hintedChunkStore
}

func newCachingValueStore(hcs hintedChunkStore) cachingValueStore {
	return cachingValueStore{newChunkTypeCache(), hcs}
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
		for _, reachable := range v.Chunks() {
			cvs.checkAndSet(reachable.TargetRef(), hintedChunk{getTargetType(reachable), r})
		}
	}
	cvs.checkAndSet(r, entry)
	return v
}
