package datas

import (
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type RemoteDataStoreClient struct {
	dataStoreCommon
}

func NewRemoteDataStore(baseURL, auth string) *RemoteDataStoreClient {
	httpBS := newHTTPBatchStore(baseURL, auth)
	return &RemoteDataStoreClient{newDataStoreCommon(types.NewValueStore(httpBS), httpBS)}
}

func (rds *RemoteDataStoreClient) batchSink() batchSink {
	httpBS := rds.vs.BatchStore().(*httpBatchStore)
	return newNotABatchSink(httpBS.host, httpBS.auth)
}

func (rds *RemoteDataStoreClient) batchStore() types.BatchStore {
	return rds.vs.BatchStore()
}

func (rds *RemoteDataStoreClient) Commit(datasetID string, commit types.Struct) (DataStore, error) {
	err := rds.commit(datasetID, commit)
	rds.vs.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.vs, rds.rt)}, err
}

func (rds *RemoteDataStoreClient) Delete(datasetID string) (DataStore, error) {
	err := rds.doDelete(datasetID)
	rds.vs.Flush()
	return &RemoteDataStoreClient{newDataStoreCommon(rds.vs, rds.rt)}, err
}

func (f RemoteStoreFactory) CreateStore(ns string) DataStore {
	return NewRemoteDataStore(f.host+httprouter.CleanPath(ns), f.auth)
}

func (f RemoteStoreFactory) Create(ns string) (DataStore, bool) {
	if ds := f.CreateStore(ns); ds != nil {
		return ds, true
	}
	return &LocalDataStore{}, false
}

func (f RemoteStoreFactory) Shutter() {}

func NewRemoteStoreFactory(host, auth string) Factory {
	return RemoteStoreFactory{host: host, auth: auth}
}

type RemoteStoreFactory struct {
	host string
	auth string
}
