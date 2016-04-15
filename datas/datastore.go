package datas

import (
	"flag"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type DataStore interface {
	// To implement types.ValueWriter, DataStore implementations provide WriteValue(). WriteValue() writes v to this DataStore, though v is not guaranteed to be be persistent until after a subsequent Commit(). The return value is the Ref of v.
	types.ValueWriter
	types.ValueReader
	io.Closer

	// MaybeHead returns the current Head Commit of this Datastore, which contains the current root of the DataStore's value tree, if available. If not, it returns a new Commit and 'false'.
	MaybeHead(datasetID string) (Commit, bool)

	// Head returns the current head Commit, which contains the current root of the DataStore's value tree.
	Head(datasetID string) Commit

	// Datasets returns the root of the datastore which is a MapOfStringToRefOfCommit where string is a datasetID.
	Datasets() MapOfStringToRefOfCommit

	// Commit updates the Commit that datasetID in this datastore points at. All Values that have been written to this DataStore are guaranteed to be persistent after Commit(). If the update cannot be performed, e.g., because of a conflict, error will non-nil. The newest snapshot of the datastore is always returned.
	Commit(datasetID string, commit Commit) (DataStore, error)

	// Delete removes the Dataset named datasetID from the map at the root of the DataStore. The Dataset data is not necessarily cleaned up at this time, but may be garbage collected in the future. If the update cannot be performed, e.g., because of a conflict, error will non-nil. The newest snapshot of the datastore is always returned.
	Delete(datasetID string) (DataStore, error)

	batchSink() batchSink
	batchStore() types.BatchStore
}

// This interface exists solely to allow RemoteDataStoreClient to pass back a gross side-channel thing for the purposes of pull.
type batchSink interface {
	SchedulePut(c chunks.Chunk, hints types.Hints)
	Flush()
	io.Closer
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return newLocalDataStore(cs)
}

type Flags struct {
	remote      remoteDataStoreFlags
	ldb         chunks.LevelDBStoreFlags
	dynamo      chunks.DynamoStoreFlags
	memory      chunks.MemoryStoreFlags
	datastoreID *string
}

func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		remoteFlags(prefix),
		chunks.LevelDBFlags(prefix),
		chunks.DynamoFlags(prefix),
		chunks.MemoryFlags(prefix),
		flag.String(prefix+"store", "", "name of datastore to access datasets in"),
	}
}

func (f Flags) CreateDataStore() (DataStore, bool) {
	if ds := f.remote.CreateStore(*f.datastoreID); ds != nil {
		return ds, true
	}

	var cs chunks.ChunkStore
	if cs = f.ldb.CreateStore(*f.datastoreID); cs != nil {
	} else if cs = f.dynamo.CreateStore(*f.datastoreID); cs != nil {
	} else if cs = f.memory.CreateStore(*f.datastoreID); cs != nil {
	}

	if cs != nil {
		return newLocalDataStore(cs), true
	}
	return &LocalDataStore{}, false
}

func (f Flags) CreateFactory() (Factory, bool) {
	if df := f.remote.CreateFactory(); df != nil {
		return df, true
	}

	var cf chunks.Factory
	if cf = f.ldb.CreateFactory(); cf != nil {
	} else if cf = f.dynamo.CreateFactory(); cf != nil {
	} else if cf = f.memory.CreateFactory(); cf != nil {
	}

	if cf != nil {
		return &localFactory{cf}, true
	}
	return &localFactory{}, false
}
