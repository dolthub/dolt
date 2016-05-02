package datas

import (
	"flag"

	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type RemoteDatabaseClient struct {
	databaseCommon
}

func NewRemoteDatabase(baseURL, auth string) *RemoteDatabaseClient {
	httpBS := newHTTPBatchStore(baseURL, auth)
	return &RemoteDatabaseClient{newDatabaseCommon(httpBS, httpBS)}
}

func (rds *RemoteDatabaseClient) batchSink() batchSink {
	httpBS := rds.bs.(*httpBatchStore)
	return newNotABatchSink(httpBS.host, httpBS.auth)
}

func (rds *RemoteDatabaseClient) batchStore() types.BatchStore {
	return rds.bs
}

func (rds *RemoteDatabaseClient) Commit(datasetID string, commit types.Struct) (Database, error) {
	err := rds.commit(datasetID, commit)
	rds.Flush()
	return &RemoteDatabaseClient{newDatabaseCommon(rds.bs, rds.rt)}, err
}

func (rds *RemoteDatabaseClient) Delete(datasetID string) (Database, error) {
	err := rds.doDelete(datasetID)
	rds.Flush()
	return &RemoteDatabaseClient{newDatabaseCommon(rds.bs, rds.rt)}, err
}

type remoteDatabaseFlags struct {
	host *string
	auth *string
}

func remoteFlags(prefix string) remoteDatabaseFlags {
	return remoteDatabaseFlags{
		flag.String(prefix+"h", "", "http host to connect to"),
		flag.String(prefix+"h-auth", "", "\"Authorization\" http header"),
	}
}

func (r remoteDatabaseFlags) CreateDatabase(ns string) Database {
	if r.check() {
		return NewRemoteDatabase(*r.host+httprouter.CleanPath(ns), *r.auth)
	}
	return nil
}

func (r remoteDatabaseFlags) Create(ns string) (Database, bool) {
	if ds := r.CreateDatabase(ns); ds != nil {
		return ds, true
	}
	return &LocalDatabase{}, false
}

func (r remoteDatabaseFlags) Shutter() {}

func (r remoteDatabaseFlags) CreateFactory() Factory {
	if r.check() {
		return r
	}
	return nil
}

func (r remoteDatabaseFlags) check() bool {
	return *r.host != ""
}
