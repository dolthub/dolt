// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type LocalDatabase struct {
	databaseCommon
	cs  chunks.ChunkStore
	vbs *localBatchStore
}

func newLocalDatabase(cs chunks.ChunkStore) *LocalDatabase {
	bs := types.NewBatchStoreAdaptor(cs)
	return &LocalDatabase{
		newDatabaseCommon(newCachingChunkHaver(cs), types.NewValueStore(bs), bs),
		cs,
		nil,
	}
}

func (ldb *LocalDatabase) GetDataset(datasetID string) Dataset {
	return getDataset(ldb, datasetID)
}

func (ldb *LocalDatabase) Commit(ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	return ldb.doHeadUpdate(
		ds,
		func(ds Dataset) error { return ldb.doCommit(ds.ID(), buildNewCommit(ds, v, opts), opts.Policy) },
	)
}

func (ldb *LocalDatabase) CommitValue(ds Dataset, v types.Value) (Dataset, error) {
	return ldb.Commit(ds, v, CommitOptions{})
}

func (ldb *LocalDatabase) Delete(ds Dataset) (Dataset, error) {
	return ldb.doHeadUpdate(ds, func(ds Dataset) error { return ldb.doDelete(ds.ID()) })
}

func (ldb *LocalDatabase) SetHead(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return ldb.doHeadUpdate(ds, func(ds Dataset) error { return ldb.doSetHead(ds, newHeadRef) })
}

func (ldb *LocalDatabase) FastForward(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return ldb.doHeadUpdate(ds, func(ds Dataset) error { return ldb.doFastForward(ds, newHeadRef) })
}

func (ldb *LocalDatabase) doHeadUpdate(ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	if ldb.vbs != nil {
		ldb.vbs.FlushAndDestroyWithoutClose()
		ldb.vbs = nil
	}
	err := updateFunc(ds)
	return ldb.GetDataset(ds.ID()), err
}

func (ldb *LocalDatabase) validatingBatchStore() types.BatchStore {
	if ldb.vbs == nil {
		ldb.vbs = newLocalBatchStore(ldb.cs)
	}
	return ldb.vbs
}

func (ldb *LocalDatabase) Close() error {
	if ldb.vbs != nil {
		ldb.vbs.Destroy()
		ldb.vbs = nil
	}
	return ldb.databaseCommon.Close()
}
