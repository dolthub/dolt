package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type LocalDataStore struct {
	dataStoreCommon
}

func newLocalDataStore(cs chunks.ChunkStore) *LocalDataStore {
	return &LocalDataStore{newDataStoreCommon(cs)}
}

func (lds *LocalDataStore) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := lds.commit(datasetID, commit)
	return newLocalDataStore(lds.cs), err
}

func (lds *LocalDataStore) Delete(datasetID string) (DataStore, error) {
	err := lds.doDelete(datasetID)
	return newLocalDataStore(lds.cs), err
}

// CopyReachableChunksP copies to |sink| all chunks reachable from (and including) |r|, but that are not in the subtree rooted at |exclude|
func (lds *LocalDataStore) CopyReachableChunksP(sourceRef, exclude ref.Ref, sink DataSink, concurrency int) {
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

	tcs := &teeDataSource{lds.cs, sink.transitionalChunkSink()}
	copyCallback := func(r ref.Ref) bool {
		return excludeRefs[r]
	}

	walk.SomeChunksP(sourceRef, tcs, copyCallback, concurrency)
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
