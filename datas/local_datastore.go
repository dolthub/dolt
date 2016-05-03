package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type LocalDataStore struct {
	cch *cachingChunkHaver
	dataStoreCommon
}

func newLocalDataStore(cs chunks.ChunkStore) *LocalDataStore {
	return &LocalDataStore{
		newCachingChunkHaver(cs),
		newDataStoreCommon(types.NewValueStore(types.NewBatchStoreAdaptor(cs)), cs),
	}
}

func (lds *LocalDataStore) Commit(datasetID string, commit types.Struct) (DataStore, error) {
	err := lds.commit(datasetID, commit)
	lds.vs.Flush()
	return &LocalDataStore{
		lds.cch,
		newDataStoreCommon(lds.vs, lds.rt),
	}, err
}

func (lds *LocalDataStore) Delete(datasetID string) (DataStore, error) {
	err := lds.doDelete(datasetID)
	lds.vs.Flush()
	return &LocalDataStore{
		lds.cch,
		newDataStoreCommon(lds.vs, lds.rt),
	}, err
}

func (lds *LocalDataStore) has(r ref.Ref) bool {
	return lds.cch.Has(r)
}

func (lds *LocalDataStore) batchSink() batchSink {
	return lds.vs.BatchStore()
}

func (lds *LocalDataStore) batchStore() types.BatchStore {
	return lds.vs.BatchStore()
}
