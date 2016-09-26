// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type LocalDatabase struct {
	databaseCommon
	cs chunks.ChunkStore
}

func newLocalDatabase(cs chunks.ChunkStore) *LocalDatabase {
	bs := types.NewBatchStoreAdaptor(cs)
	return &LocalDatabase{
		newDatabaseCommon(newCachingChunkHaver(cs), types.NewValueStore(bs), bs),
		cs,
	}
}

func (ldb *LocalDatabase) GetDataset(datasetID string) Dataset {
	return getDataset(ldb, datasetID)
}

func (ldb *LocalDatabase) Commit(ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	err := ldb.doCommit(ds.ID(), buildNewCommit(ds, v, opts))
	return ldb.GetDataset(ds.ID()), err
}

func (ldb *LocalDatabase) CommitValue(ds Dataset, v types.Value) (Dataset, error) {
	return ldb.Commit(ds, v, CommitOptions{})
}

func (ldb *LocalDatabase) Delete(ds Dataset) (Dataset, error) {
	err := ldb.doDelete(ds.ID())
	return ldb.GetDataset(ds.ID()), err
}

func (ldb *LocalDatabase) SetHead(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	err := ldb.doSetHead(ds, newHeadRef)
	return ldb.GetDataset(ds.ID()), err
}

func (ldb *LocalDatabase) FastForward(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	err := ldb.doFastForward(ds, newHeadRef)
	return ldb.GetDataset(ds.ID()), err
}

func (ldb *LocalDatabase) validatingBatchStore() (bs types.BatchStore) {
	bs = ldb.ValueStore.BatchStore()
	if !bs.IsValidating() {
		bs = newLocalBatchStore(ldb.cs)
		ldb.ValueStore = types.NewValueStore(bs)
		ldb.rt = bs
	}
	d.PanicIfFalse(bs.IsValidating())
	return bs
}
