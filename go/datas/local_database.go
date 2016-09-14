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

func (lds *LocalDatabase) Commit(datasetID string, commit types.Struct) (Database, error) {
	err := lds.doCommit(datasetID, commit)
	return &LocalDatabase{newDatabaseCommon(lds.cch, lds.ValueStore, lds.rt), lds.cs}, err
}

func (lds *LocalDatabase) Delete(datasetID string) (Database, error) {
	err := lds.doDelete(datasetID)
	return &LocalDatabase{newDatabaseCommon(lds.cch, lds.ValueStore, lds.rt), lds.cs}, err
}

func (lds *LocalDatabase) SetHead(datasetID string, commit types.Struct) (Database, error) {
	err := lds.doSetHead(datasetID, commit)
	return &LocalDatabase{newDatabaseCommon(lds.cch, lds.ValueStore, lds.rt), lds.cs}, err
}

func (lds *LocalDatabase) validatingBatchStore() (bs types.BatchStore) {
	bs = lds.ValueStore.BatchStore()
	if !bs.IsValidating() {
		bs = newLocalBatchStore(lds.cs)
		lds.ValueStore = types.NewValueStore(bs)
		lds.rt = bs
	}
	d.PanicIfFalse(bs.IsValidating())
	return bs
}
