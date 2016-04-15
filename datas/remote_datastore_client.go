package datas

import (
	"flag"
	"net/url"

	"github.com/julienschmidt/httprouter"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type RemoteDataStoreClient struct {
	dataStoreCommon
}

func NewRemoteDataStore(baseURL, auth string) *RemoteDataStoreClient {
	return &RemoteDataStoreClient{newDataStoreCommon(newHTTPHintedChunkStore(baseURL, auth))}
}

func (rds *RemoteDataStoreClient) host() *url.URL {
	return rds.hcs.(*httpHintedChunkStore).host
}

func (rds *RemoteDataStoreClient) hintedChunkSink() hintedChunkSink {
	hhcs := rds.hcs.(*httpHintedChunkStore)
	return newNotAHintedChunkStore(hhcs.host, hhcs.auth)
}

func (rds *RemoteDataStoreClient) hintedChunkStore() hintedChunkStore {
	return rds.hcs
}

func (rds *RemoteDataStoreClient) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := rds.commit(datasetID, commit)
	rds.hcs.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.hcs)}, err
}

func (rds *RemoteDataStoreClient) Delete(datasetID string) (DataStore, error) {
	err := rds.doDelete(datasetID)
	rds.hcs.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.hcs)}, err
}

type remoteDataStoreFlags struct {
	host *string
	auth *string
}

func remoteFlags(prefix string) remoteDataStoreFlags {
	return remoteDataStoreFlags{
		flag.String(prefix+"h", "", "http host to connect to"),
		flag.String(prefix+"h-auth", "", "\"Authorization\" http header"),
	}
}

func (r remoteDataStoreFlags) CreateStore(ns string) DataStore {
	if r.check() {
		return NewRemoteDataStore(*r.host+httprouter.CleanPath(ns), *r.auth)
	}
	return nil
}

func (r remoteDataStoreFlags) Create(ns string) (DataStore, bool) {
	if ds := r.CreateStore(ns); ds != nil {
		return ds, true
	}
	return &LocalDataStore{}, false
}

func (r remoteDataStoreFlags) Shutter() {}

func (r remoteDataStoreFlags) CreateFactory() Factory {
	if r.check() {
		return r
	}
	return nil
}

func (r remoteDataStoreFlags) check() bool {
	return *r.host != ""
}
