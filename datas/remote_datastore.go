package datas

import (
	"net/url"

	"github.com/attic-labs/noms/chunks"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type RemoteDataStore struct {
	dataStoreCommon
}

func newRemoteDataStore(cs chunks.ChunkStore) *RemoteDataStore {
	return &RemoteDataStore{newDataStoreCommon(cs)}
}

func (rds *RemoteDataStore) host() *url.URL {
	return rds.cs.(*chunks.HTTPStore).Host()
}

func (rds *RemoteDataStore) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := rds.commit(datasetID, commit)
	return newRemoteDataStore(rds.cs), err
}

func (rds *RemoteDataStore) Delete(datasetID string) (DataStore, error) {
	err := rds.doDelete(datasetID)
	return newRemoteDataStore(rds.cs), err
}
