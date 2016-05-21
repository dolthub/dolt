package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/hash"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// CopyMissingChunksP copies to |sink| all chunks in source that are reachable from (and including) |r|, skipping chunks that |sink| already has
func CopyMissingChunksP(source Database, sink *LocalDatabase, sourceRef types.Ref, concurrency int) {
	stopCallback := func(r types.Ref) bool {
		return sink.has(r.TargetHash())
	}
	copyWorker(source, sink, sourceRef, stopCallback, concurrency)
}

// CopyReachableChunksP copies to |sink| all chunks reachable from (and including) |r|, but that are not in the subtree rooted at |exclude|
func CopyReachableChunksP(source, sink Database, sourceRef, exclude types.Ref, concurrency int) {
	excludeRefs := map[hash.Hash]bool{}

	if !exclude.TargetHash().IsEmpty() {
		mu := sync.Mutex{}
		excludeCallback := func(r types.Ref) bool {
			mu.Lock()
			defer mu.Unlock()
			excludeRefs[r.TargetHash()] = true
			return false
		}

		walk.SomeChunksP(exclude, source.batchStore(), excludeCallback, nil, concurrency)
	}

	stopCallback := func(r types.Ref) bool {
		return excludeRefs[r.TargetHash()]
	}
	copyWorker(source, sink, sourceRef, stopCallback, concurrency)
}

func copyWorker(source, sink Database, sourceRef types.Ref, stopCb walk.SomeChunksStopCallback, concurrency int) {
	bs := sink.batchSink()

	walk.SomeChunksP(sourceRef, source.batchStore(), stopCb, func(r types.Ref, c chunks.Chunk) {
		bs.SchedulePut(c, r.Height(), types.Hints{})
	}, concurrency)

	bs.Flush()
}
