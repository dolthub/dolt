package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
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
		newDataStoreCommon(types.NewBatchStoreAdaptor(cs), cs),
	}
}

func (lds *LocalDataStore) Commit(datasetID string, commit types.Struct) (DataStore, error) {
	err := lds.commit(datasetID, commit)
	lds.Flush()
	return newLocalDataStore(lds.cs), err
}

func (lds *LocalDataStore) Delete(datasetID string) (DataStore, error) {
	err := lds.doDelete(datasetID)
	lds.Flush()
	return newLocalDataStore(lds.cs), err
}

func (lds *LocalDataStore) has(r ref.Ref) bool {
	return lds.cch.Has(r)
}

func (lds *LocalDataStore) batchSink() batchSink {
	return lds.bs
}

func (lds *LocalDataStore) batchStore() types.BatchStore {
	return lds.bs
}
