package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// CopyMissingChunksP copies to |sink| all chunks in source that are reachable from (and including) |r|, skipping chunks that |sink| already has
func CopyMissingChunksP(source DataStore, sink *LocalDataStore, sourceRef ref.Ref, concurrency int) {
	sinkCS := sink.transitionalChunkStore()
	copyCallback := func(r ref.Ref) bool {
		return sinkCS.Has(r)
	}
	copyWorker(source, sinkCS, sourceRef, copyCallback, concurrency)
}

// CopyReachableChunksP copies to |sink| all chunks reachable from (and including) |r|, but that are not in the subtree rooted at |exclude|
func CopyReachableChunksP(source, sink DataStore, sourceRef, exclude ref.Ref, concurrency int) {
	excludeRefs := map[ref.Ref]bool{}

	if !exclude.IsEmpty() {
		mu := sync.Mutex{}
		excludeCallback := func(r ref.Ref) bool {
			mu.Lock()
			excludeRefs[r] = true
			mu.Unlock()
			return false
		}

		walk.SomeChunksP(exclude, source, excludeCallback, concurrency)
	}

	copyCallback := func(r ref.Ref) bool {
		return excludeRefs[r]
	}
	copyWorker(source, sink.transitionalChunkSink(), sourceRef, copyCallback, concurrency)
}

func copyWorker(source DataStore, sink chunks.ChunkSink, sourceRef ref.Ref, stopFn walk.SomeChunksCallback, concurrency int) {
	tcs := &teeDataSource{source.transitionalChunkStore(), sink}
	walk.SomeChunksP(sourceRef, tcs, stopFn, concurrency)
}

// teeDataSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeDataSource struct {
	source chunks.ChunkSource
	sink   chunks.ChunkSink
}

func (trs *teeDataSource) ReadValue(ref ref.Ref) types.Value {
	c := trs.source.Get(ref)
	if c.IsEmpty() {
		return nil
	}

	trs.sink.Put(c)
	return types.DecodeChunk(c, trs)
}
