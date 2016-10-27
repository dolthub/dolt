// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/julienschmidt/httprouter"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type RemoteDatabaseClient struct {
	databaseCommon
}

func NewRemoteDatabase(baseURL, auth string) *RemoteDatabaseClient {
	httpBS := newHTTPBatchStore(baseURL, auth)
	return &RemoteDatabaseClient{newDatabaseCommon(newCachingChunkHaver(httpBS), types.NewValueStore(httpBS), httpBS)}
}

func (rdb *RemoteDatabaseClient) validatingBatchStore() (bs types.BatchStore) {
	bs = rdb.ValueStore.BatchStore()
	return
}

func (rdb *RemoteDatabaseClient) GetDataset(datasetID string) Dataset {
	return getDataset(rdb, datasetID)
}

func (rdb *RemoteDatabaseClient) Commit(ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	err := rdb.doCommit(ds.ID(), buildNewCommit(ds, v, opts), opts.Policy)
	return rdb.GetDataset(ds.ID()), err
}

func (rdb *RemoteDatabaseClient) CommitValue(ds Dataset, v types.Value) (Dataset, error) {
	return rdb.Commit(ds, v, CommitOptions{})
}

func (rdb *RemoteDatabaseClient) Delete(ds Dataset) (Dataset, error) {
	err := rdb.doDelete(ds.ID())
	return rdb.GetDataset(ds.ID()), err
}

func (rdb *RemoteDatabaseClient) SetHead(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	err := rdb.doSetHead(ds, newHeadRef)
	return rdb.GetDataset(ds.ID()), err
}

func (rdb *RemoteDatabaseClient) FastForward(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	err := rdb.doFastForward(ds, newHeadRef)
	return rdb.GetDataset(ds.ID()), err
}

func (f RemoteStoreFactory) CreateStore(ns string) Database {
	return NewRemoteDatabase(f.host+httprouter.CleanPath(ns), f.auth)
}

func (f RemoteStoreFactory) Create(ns string) (Database, bool) {
	if ds := f.CreateStore(ns); ds != nil {
		return ds, true
	}
	return &LocalDatabase{}, false
}

func (f RemoteStoreFactory) Shutter() {}

func NewRemoteStoreFactory(host, auth string) Factory {
	return RemoteStoreFactory{host: host, auth: auth}
}

type RemoteStoreFactory struct {
	host string
	auth string
}
