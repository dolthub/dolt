package datas

import (
	"flag"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type Database interface {
	// To implement types.ValueWriter, Database implementations provide WriteValue(). WriteValue() writes v to this Database, though v is not guaranteed to be be persistent until after a subsequent Commit(). The return value is the Ref of v.
	types.ValueWriter
	types.ValueReader
	io.Closer

	// MaybeHead returns the current Head Commit of this Database, which contains the current root of the Database's value tree, if available. If not, it returns a new Commit and 'false'.
	MaybeHead(datasetID string) (types.Struct, bool)

	// Head returns the current head Commit, which contains the current root of the Database's value tree.
	Head(datasetID string) types.Struct

	// Datasets returns the root of the database which is a MapOfStringToRefOfCommit where string is a datasetID.
	Datasets() types.Map

	// Commit updates the Commit that datasetID in this database points at. All Values that have been written to this Database are guaranteed to be persistent after Commit(). If the update cannot be performed, e.g., because of a conflict, error will non-nil. The newest snapshot of the database is always returned.
	Commit(datasetID string, commit types.Struct) (Database, error)

	// Delete removes the Dataset named datasetID from the map at the root of the Database. The Dataset data is not necessarily cleaned up at this time, but may be garbage collected in the future. If the update cannot be performed, e.g., because of a conflict, error will non-nil. The newest snapshot of the database is always returned.
	Delete(datasetID string) (Database, error)

	batchSink() batchSink
	batchStore() types.BatchStore
}

// This interface exists solely to allow RemoteDatabaseClient to pass back a gross side-channel thing for the purposes of pull.
type batchSink interface {
	SchedulePut(c chunks.Chunk, hints types.Hints)
	Flush()
	io.Closer
}

func NewDatabase(cs chunks.ChunkStore) Database {
	return newLocalDatabase(cs)
}

type Flags struct {
	remote     remoteDatabaseFlags
	ldb        chunks.LevelDBStoreFlags
	dynamo     chunks.DynamoStoreFlags
	memory     chunks.MemoryStoreFlags
	databaseID *string
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
		flag.String(prefix+"database", "", "name of database to access datasets in"),
	}
}

func (f Flags) CreateDatabase() (Database, bool) {
	if ds := f.remote.CreateDatabase(*f.databaseID); ds != nil {
		return ds, true
	}

	var cs chunks.ChunkStore
	if cs = f.ldb.CreateStore(*f.databaseID); cs != nil {
	} else if cs = f.dynamo.CreateStore(*f.databaseID); cs != nil {
	} else if cs = f.memory.CreateStore(*f.databaseID); cs != nil {
	}

	if cs != nil {
		return newLocalDatabase(cs), true
	}
	return &LocalDatabase{}, false
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
