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

	rt    chunks.RootTracker
	cc    *commitCache
	heads SetOfCommit
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return NewDataStoreWithRootTracker(cs, cs)
}

// NewDataStore() creates a new DataStore with a specified ChunkStore and RootTracker. Typically these two values will be the same, but it is sometimes useful to have a separate RootTracker (e.g., see DataSet).
func NewDataStoreWithRootTracker(cs chunks.ChunkStore, rt chunks.RootTracker) DataStore {
	return newDataStoreInternal(cs, rt, newCommitCache(cs))
}

func newDataStoreInternal(cs chunks.ChunkStore, rt chunks.RootTracker, cc *commitCache) DataStore {
	if (rt.Root() == ref.Ref{}) {
		r := types.WriteValue(NewSetOfCommit().NomsValue(), cs)
		d.Chk.True(rt.UpdateRoot(r, ref.Ref{}))
	}
	return DataStore{
		cs, rt, cc, commitSetFromRef(rt.Root(), cs),
	}
}

func commitSetFromRef(commitRef ref.Ref, cs chunks.ChunkSource) SetOfCommit {
	return SetOfCommitFromVal(types.ReadValue(commitRef, cs))
}

// Heads returns the head Commit of all currently active forks.
func (ds *DataStore) Heads() SetOfCommit {
	return ds.heads
}

// Commit returns a new DataStore with newCommits as the heads, but backed by the same ChunkStore and RootTracker instances as the current one.
func (ds *DataStore) Commit(newCommits SetOfCommit) DataStore {
	d.Chk.True(newCommits.Len() > 0)
	// TODO: We probably shouldn't let this go *forever*. Consider putting a limit and... I know don't...panicing?
	for !ds.doCommit(newCommits) {
	}
	return newDataStoreInternal(ds.ChunkStore, ds.rt, ds.cc)
}

// doCommit manages concurrent access the single logical piece of mutable state: the set of current heads. doCommit is optimistic in that it is attempting to update heads making the assumption that currentRootRef is the ref of the current heads. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorigthm must be tried again.
func (ds *DataStore) doCommit(commits SetOfCommit) bool {
	d.Chk.True(commits.Len() > 0)

	currentRootRef := ds.rt.Root()

	// Note: |currentHeads| may be different from |ds.heads| and *must* be consistent with |currentCommitRef|.
	var currentHeads SetOfCommit
	if currentRootRef == ds.heads.Ref() {
		currentHeads = ds.heads
	} else {
		currentHeads = commitSetFromRef(currentRootRef, ds)
	}

	newHeads := commits.Union(currentHeads)

	commits.Iter(func(commit Commit) (stop bool) {
		if ds.isPrexisting(commit, currentHeads) {
			newHeads = newHeads.Remove(commit)
		} else {
			newHeads = SetOfCommitFromVal(newHeads.NomsValue().Subtract(commit.Parents()))
		}
		return
	})

	if newHeads.Len() == 0 || newHeads.Equals(currentHeads) {
		return true
	}

	// TODO: This set will be orphaned if this UpdateRoot below fails
	newRootRef := types.WriteValue(newHeads.NomsValue(), ds)

	return ds.rt.UpdateRoot(newRootRef, currentRootRef)
}

func (ds *DataStore) isPrexisting(commit Commit, currentHeads SetOfCommit) bool {
	if currentHeads.Has(commit) {
		return true
	}

	// If a new commit directly superceeds an existing current commit, it can't have already been committed because its hash would be uncomputable.
	superceedsCurrentCommit := false
	commit.Parents().Iter(func(parent types.Value) (stop bool) {
		superceedsCurrentCommit = currentHeads.Has(CommitFromVal(parent))
		return superceedsCurrentCommit
	})
	if superceedsCurrentCommit {
		return false
	}

	ds.cc.Update(currentHeads)
	return ds.cc.Contains(commit.Ref())
}
