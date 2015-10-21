package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type DataStore interface {
	chunks.ChunkStore

	// MaybeHead returns the current Head Commit of this Datastore, which contains the current root of the DataStore's value tree, if available. If not, it returns a new Commit and 'false'.
	MaybeHead(datasetID string) (Commit, bool)

	// Head returns the current head Commit, which contains the current root of the DataStore's value tree.
	Head(datasetID string) Commit

	// Datasets returns the root of the datastore which is a MapOfStringToRefOfCommit where string is a datasetID.
	Datasets() MapOfStringToRefOfCommit

	// Commit updates the commit that a datastore points at. The new Commit is constructed using v and the current Head. If the update cannot be performed, e.g., because of a conflict, Commit returns 'false'. The newest snapshot of the datastore is always returned.
	Commit(datasetID string, commit Commit) (DataStore, bool)

	CopyReachableChunksP(r, exclude ref.Ref, sink chunks.ChunkSink, concurrency int)
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return newLocalDataStore(cs)
}

type Flags struct {
	ldb    chunks.LevelDBStoreFlags
	memory chunks.MemoryStoreFlags
	hflags chunks.HttpStoreFlags
}

func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		chunks.LevelDBFlags(prefix),
		chunks.MemoryFlags(prefix),
		chunks.HttpFlags(prefix),
	}
}

func (f Flags) CreateDataStore() (DataStore, bool) {
	var cs chunks.ChunkStore
	if cs = f.ldb.CreateStore(); cs != nil {
	} else if cs = f.memory.CreateStore(); cs != nil {
	}

	if cs != nil {
		return newLocalDataStore(cs), true
	}

	if cs = f.hflags.CreateStore(); cs != nil {
		return newRemoteDataStore(cs), true
	}

	return &LocalDataStore{}, false
}
