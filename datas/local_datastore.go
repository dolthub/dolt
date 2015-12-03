package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/walk"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type LocalDataStore struct {
	dataStoreCommon
}

func newLocalDataStore(cs chunks.ChunkStore) *LocalDataStore {
	rootRef := cs.Root()
	if rootRef.IsEmpty() {
		return &LocalDataStore{dataStoreCommon{cs, nil}}
	}

	return &LocalDataStore{dataStoreCommon{cs, datasetsFromRef(rootRef, cs)}}
}

func (lds *LocalDataStore) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := lds.commit(datasetID, commit)
	return newLocalDataStore(lds.ChunkStore), err
}

// Copies all chunks reachable from (and including)|sourceRef| but not reachable from (and including) |exclude| in |source| to |sink|
func (lds *LocalDataStore) CopyReachableChunksP(sourceRef, exclude ref.Ref, sink chunks.ChunkSink, concurrency int) {
	excludeRefs := map[ref.Ref]bool{}

	if !exclude.IsEmpty() {
		mu := sync.Mutex{}
		excludeCallback := func(r ref.Ref) bool {
			mu.Lock()
			excludeRefs[r] = true
			mu.Unlock()
			return false
		}

		walk.SomeChunksP(exclude, lds, excludeCallback, concurrency)
	}

	tcs := &teeChunkSource{lds, sink}
	copyCallback := func(r ref.Ref) bool {
		return excludeRefs[r]
	}

	walk.SomeChunksP(sourceRef, tcs, copyCallback, concurrency)
}

// teeChunkSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeChunkSource struct {
	source chunks.ChunkSource
	sink   chunks.ChunkSink
}

func (trs *teeChunkSource) Get(ref ref.Ref) chunks.Chunk {
	c := trs.source.Get(ref)
	if c.IsEmpty() {
		return c
	}

	trs.sink.Put(c)
	return c
}

func (trs *teeChunkSource) Has(ref ref.Ref) bool {
	return trs.source.Has(ref)
}

func (trs *teeChunkSource) Root() ref.Ref {
	panic("not reached")
}

func (trs *teeChunkSource) UpdateRoot(current, existing ref.Ref) bool {
	panic("not reached")
}

func (trs *teeChunkSource) Put(c chunks.Chunk) {
	panic("not reached")
}

func (trs *teeChunkSource) Close() error {
	panic("not reached")
}
