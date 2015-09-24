package datas

import (
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
	rootRef := cs.Root()
	if rootRef == (ref.Ref{}) {
		return &LocalDataStore{dataStoreCommon{cs, nil}}
	}

	return &LocalDataStore{dataStoreCommon{cs, commitFromRef(rootRef, cs)}}
}

func (lds *LocalDataStore) Commit(v types.Value) (DataStore, bool) {
	ok := lds.commit(v)
	return newLocalDataStore(lds.ChunkStore), ok
}

func (lds *LocalDataStore) CommitWithParents(v types.Value, p SetOfCommit) (DataStore, bool) {
	ok := lds.commitWithParents(v, p)
	return newLocalDataStore(lds.ChunkStore), ok
}

// Copys all chunks reachable from (and including) |r| but excluding all chunks reachable from (and including) |exclude| in |source| to |sink|.
func (lds *LocalDataStore) CopyReachableChunksP(r, exclude ref.Ref, sink chunks.ChunkSink, concurrency int) {
	excludeRefs := map[ref.Ref]bool{}
	hasRef := func(r ref.Ref) bool {
		return excludeRefs[r]
	}

	if exclude != (ref.Ref{}) {
		refChan := make(chan ref.Ref, 1024)
		addRef := func(r ref.Ref) {
			refChan <- r
		}

		go func() {
			walk.AllP(exclude, lds, addRef, concurrency)
			close(refChan)
		}()

		for r := range refChan {
			excludeRefs[r] = true
		}
	}

	tcs := &teeChunkSource{lds, sink}
	walk.SomeP(r, tcs, hasRef, concurrency)
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
