package datas

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type DataStore struct {
	chunks.ChunkStore

	rt    chunks.RootTracker
	rc    *commitCache
	heads SetOfCommit
}

func NewDataStore(cs chunks.ChunkStore, rt chunks.RootTracker) DataStore {
	return newDataStoreInternal(cs, rt, NewCommitCache(cs))
}

func newDataStoreInternal(cs chunks.ChunkStore, rt chunks.RootTracker, rc *commitCache) DataStore {
	return DataStore{
		cs, rt, rc, commitSetFromRef(rt.Root(), cs),
	}
}

func commitSetFromRef(commitRef ref.Ref, cs chunks.ChunkSource) SetOfCommit {
	var commits SetOfCommit
	if (commitRef == ref.Ref{}) {
		commits = NewSetOfCommit()
	} else {
		commits = SetOfCommitFromVal(types.MustReadValue(commitRef, cs).(types.Set))
	}

	return commits
}

func (ds *DataStore) Heads() SetOfCommit {
	return ds.heads
}

func (ds *DataStore) Commit(newCommits SetOfCommit) DataStore {
	Chk.True(newCommits.Len() > 0)
	// TODO: We probably shouldn't let this go *forever*. Considrer putting a limit and... I know don't...panicing?
	for !ds.doCommit(newCommits) {
	}
	return newDataStoreInternal(ds.ChunkStore, ds.rt, ds.rc)
}

// doCommit manages concurrent access the single logical piece of mutable state: the set of current heads. doCommit is optimistic in that it is attempting to update heads making the assumption that currentRootRef is the ref of the current heads. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorigthm must be tried again.
func (ds *DataStore) doCommit(commits SetOfCommit) bool {
	Chk.True(commits.Len() > 0)

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

	// TODO: This set will be orphaned if this UpdateCommit below fails
	newRootRef, err := types.WriteValue(newHeads.NomsValue(), ds)
	Chk.NoError(err)

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

	ds.rc.Update(currentHeads)
	return ds.rc.Contains(commit.Ref())
}
