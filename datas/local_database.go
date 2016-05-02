package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type LocalDatabase struct {
	cs  chunks.ChunkStore
	cch *cachingChunkHaver
	databaseCommon
}

func newLocalDatabase(cs chunks.ChunkStore) *LocalDatabase {
	return &LocalDatabase{
		cs,
		newCachingChunkHaver(cs),
		newDatabaseCommon(types.NewBatchStoreAdaptor(cs), cs),
	}
}

func (lds *LocalDatabase) Commit(datasetID string, commit types.Struct) (Database, error) {
	err := lds.commit(datasetID, commit)
	lds.Flush()
	return newLocalDatabase(lds.cs), err
}

func (lds *LocalDatabase) Delete(datasetID string) (Database, error) {
	err := lds.doDelete(datasetID)
	lds.Flush()
	return newLocalDatabase(lds.cs), err
}

func (lds *LocalDatabase) has(r ref.Ref) bool {
	return lds.cch.Has(r)
}

func (lds *LocalDatabase) batchSink() batchSink {
	return lds.bs
}

func (lds *LocalDatabase) batchStore() types.BatchStore {
	return lds.bs
}
