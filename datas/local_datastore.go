package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type LocalDataStore struct {
	DataStoreCommon
}

func newLocalDataStore(cs chunks.ChunkStore) *LocalDataStore {
	rootRef := cs.Root()
	if rootRef == (ref.Ref{}) {
		return &LocalDataStore{DataStoreCommon{cs, nil}}
	}

	return &LocalDataStore{DataStoreCommon{cs, commitFromRef(rootRef, cs)}}
}

func newDataStoreInternal(cs chunks.ChunkStore) DataStoreCommon {
	if (cs.Root() == ref.Ref{}) {
		return DataStoreCommon{cs, nil}
	}
	return DataStoreCommon{cs, commitFromRef(cs.Root(), cs)}
}

func (lds *LocalDataStore) Commit(v types.Value) (DataStore, bool) {
	ok := lds.commit(v)
	return newLocalDataStore(lds.ChunkStore), ok
}

func (lds *LocalDataStore) CommitWithParents(v types.Value, p SetOfCommit) (DataStore, bool) {
	ok := lds.commitWithParents(v, p)
	return newLocalDataStore(lds.ChunkStore), ok
}
