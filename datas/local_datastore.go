package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type LocalDataStore struct {
	cs  chunks.ChunkStore
	cch *cachingChunkHaver
	dataStoreCommon
}

func newLocalDataStore(cs chunks.ChunkStore) *LocalDataStore {
	return &LocalDataStore{
		cs,
		newCachingChunkHaver(cs),
		newDataStoreCommon(&naiveHintedChunkStore{cs}),
	}
}

func (lds *LocalDataStore) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := lds.commit(datasetID, commit)
	lds.hcs.Flush()
	return newLocalDataStore(lds.cs), err
}

func (lds *LocalDataStore) Delete(datasetID string) (DataStore, error) {
	err := lds.doDelete(datasetID)
	lds.hcs.Flush()
	return newLocalDataStore(lds.cs), err
}

func (lds *LocalDataStore) has(r ref.Ref) bool {
	return lds.cch.Has(r)
}

func (lds *LocalDataStore) hintedChunkSink() hintedChunkSink {
	return lds.hcs
}

func (lds *LocalDataStore) hintedChunkStore() hintedChunkStore {
	return lds.hcs
}
