package datas

import (
	"flag"

	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type RemoteDataStoreClient struct {
	dataStoreCommon
}

func newRemoteDataStore(baseURL, auth string) *RemoteDataStoreClient {
	httpBS := newHTTPBatchStore(baseURL, auth)
	return &RemoteDataStoreClient{newDataStoreCommon(httpBS, httpBS)}
}

func (rds *RemoteDataStoreClient) batchSink() batchSink {
	httpBS := rds.bs.(*httpBatchStore)
	return newNotABatchSink(httpBS.host, httpBS.auth)
}

func (rds *RemoteDataStoreClient) batchStore() types.BatchStore {
	return rds.bs
}

func (rds *RemoteDataStoreClient) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := rds.commit(datasetID, commit)
	rds.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.bs, rds.rt)}, err
}

func (rds *RemoteDataStoreClient) Delete(datasetID string) (DataStore, error) {
	err := rds.doDelete(datasetID)
	rds.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.bs, rds.rt)}, err
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
		return newRemoteDataStore(*r.host+httprouter.CleanPath(ns), *r.auth)
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
