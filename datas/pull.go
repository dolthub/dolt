package datas

import (
	"sync"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// CopyMissingChunksP copies to |sink| all chunks in source that are reachable from (and including) |r|, skipping chunks that |sink| already has
func CopyMissingChunksP(source DataStore, sink *LocalDataStore, sourceRef ref.Ref, concurrency int) {
	copyCallback := func(r ref.Ref) bool {
		return sink.has(r)
	}
	copyWorker(source, sink, sourceRef, copyCallback, concurrency)
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
	copyWorker(source, sink, sourceRef, copyCallback, concurrency)
}

func copyWorker(source DataStore, sink DataStore, sourceRef ref.Ref, stopFn walk.SomeChunksCallback, concurrency int) {
	hcs := sink.hintedChunkSink()
	walk.SomeChunksP(sourceRef, newTeeDataSource(source.hintedChunkStore(), hcs), stopFn, concurrency)

	hcs.Flush()
}

// teeDataSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeDataSource struct {
	source hintedChunkStore
	sink   hintedChunkSink
}

func newTeeDataSource(source hintedChunkStore, sink hintedChunkSink) *teeDataSource {
	return &teeDataSource{source, sink}
}

func (tds *teeDataSource) ReadValue(r ref.Ref) types.Value {
	c := tds.source.Get(r)
	if c.IsEmpty() {
		return nil
	}
	tds.sink.Put(c, map[ref.Ref]struct{}{})
	return types.DecodeChunk(c, tds)
}
