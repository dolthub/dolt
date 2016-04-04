package datas

import "github.com/attic-labs/noms/chunks"

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
