package datas

import (
	"sync"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// CopyMissingChunksP copies to |sink| all chunks in source that are reachable from (and including) |r|, skipping chunks that |sink| already has
func CopyMissingChunksP(source DataStore, sink *LocalDataStore, sourceRef types.RefBase, concurrency int) {
	copyCallback := func(r types.RefBase) bool {
		return sink.has(r.TargetRef())
	}
	copyWorker(source, sink, sourceRef, copyCallback, concurrency)
}

// CopyReachableChunksP copies to |sink| all chunks reachable from (and including) |r|, but that are not in the subtree rooted at |exclude|
func CopyReachableChunksP(source, sink DataStore, sourceRef, exclude types.RefBase, concurrency int) {
	excludeRefs := map[ref.Ref]bool{}

	if !exclude.TargetRef().IsEmpty() {
		mu := sync.Mutex{}
		excludeCallback := func(r types.RefBase) bool {
			mu.Lock()
			excludeRefs[r.TargetRef()] = true
			mu.Unlock()
			return false
		}

		walk.SomeChunksP(exclude, source, excludeCallback, concurrency)
	}

	copyCallback := func(r types.RefBase) bool {
		return excludeRefs[r.TargetRef()]
	}
	copyWorker(source, sink, sourceRef, copyCallback, concurrency)
}

func copyWorker(source DataStore, sink DataStore, sourceRef types.RefBase, stopFn walk.SomeChunksCallback, concurrency int) {
	bs := sink.batchSink()
	walk.SomeChunksP(sourceRef, newTeeDataSource(source.batchStore(), bs), stopFn, concurrency)

	bs.Flush()
}

// teeDataSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeDataSource struct {
	source types.BatchStore
	sink   batchSink
}

func newTeeDataSource(source types.BatchStore, sink batchSink) *teeDataSource {
	return &teeDataSource{source, sink}
}

func (tds *teeDataSource) ReadValue(r ref.Ref) types.Value {
	c := tds.source.Get(r)
	if c.IsEmpty() {
		return nil
	}
	tds.sink.SchedulePut(c, types.Hints{})
	return types.DecodeChunk(c, tds)
}
