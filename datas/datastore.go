package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type DataStore struct {
	chunks.ChunkStore
	head Commit
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return newDataStoreInternal(cs)
}

var EmptyCommit = NewCommit().SetParents(NewSetOfCommit().NomsValue())

func newDataStoreInternal(cs chunks.ChunkStore) DataStore {
	if (cs.Root() == ref.Ref{}) {
		r := types.WriteValue(EmptyCommit.NomsValue(), cs) // this is a little weird.
		d.Chk.True(cs.UpdateRoot(r, ref.Ref{}))
	}
	return DataStore{cs, commitFromRef(cs.Root(), cs)}
}

func commitFromRef(commitRef ref.Ref, cs chunks.ChunkSource) Commit {
	return CommitFromVal(types.ReadValue(commitRef, cs))
}

// Head returns the current head Commit, which contains the current root of the DataStore's value tree.
func (ds *DataStore) Head() Commit {
	return ds.head
}

// HeadAsSet returns a types.Set containing only ds.Head(). This is a common need and, currently, pretty verbose.
func (ds *DataStore) HeadAsSet() types.Set {
	return NewSetOfCommit().Insert(ds.Head()).NomsValue()
}

// Commit returns a new DataStore with newCommit as the head, but backed by the same ChunkStore and RootTracker instances as the current one.
// newCommit MUST descend from the current Head().
// If the call fails, the boolean return value will be set to false and the caller must retry. Regardless, the DataStore returned is the right one to use for subsequent calls to Commit() -- retries or otherwise.
func (ds *DataStore) Commit(newCommit Commit) (DataStore, bool) {
	ok := ds.doCommit(newCommit)
	return newDataStoreInternal(ds.ChunkStore), ok
}

// doCommit manages concurrent access the single logical piece of mutable state: the current head. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *DataStore) doCommit(commit Commit) bool {
	currentRootRef := ds.Root()

	// Note: |currentHead| may be different from |ds.head| and *must* be consistent with |currentRootRef|.
	var currentHead Commit
	if currentRootRef == ds.head.Ref() {
		currentHead = ds.head
	} else {
		currentHead = commitFromRef(currentRootRef, ds)
	}

	// Allow only fast-forward commits.
	if commit.Equals(currentHead) {
		return true
	} else if !descendsFrom(commit, currentHead) {
		return false
	}

	// TODO: This Commit will be orphaned if this UpdateRoot below fails
	newRootRef := types.WriteValue(commit.NomsValue(), ds)

	ok := ds.UpdateRoot(newRootRef, currentRootRef)
	return ok
}

func descendsFrom(commit, currentHead Commit) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := NewSetOfCommit().Insert(commit)
	for !ancestors.Has(currentHead) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors)
	}
	return true
}

func getAncestors(commits SetOfCommit) SetOfCommit {
	ancestors := NewSetOfCommit()
	commits.Iter(func(c Commit) (stop bool) {
		ancestors =
			ancestors.Union(SetOfCommitFromVal(c.Parents()))
		return
	})
	return ancestors
}
