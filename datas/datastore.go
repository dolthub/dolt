package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/http"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type DataStore struct {
	chunks.ChunkStore
	head *Commit
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return newDataStoreInternal(cs)
}

func newDataStoreInternal(cs chunks.ChunkStore) DataStore {
	if (cs.Root() == ref.Ref{}) {
		return DataStore{cs, nil}
	}
	return DataStore{cs, commitFromRef(cs.Root(), cs)}
}

func commitFromRef(commitRef ref.Ref, cs chunks.ChunkSource) *Commit {
	c := CommitFromVal(types.ReadValue(commitRef, cs))
	return &c
}

// MaybeHead returns the current Head Commit of this Datastore, which contains the current root of the DataStore's value tree, if available. If not, it returns a new Commit and 'false'.
func (ds *DataStore) MaybeHead() (Commit, bool) {
	if ds.head == nil {
		return NewCommit(), false
	}
	return *ds.head, true
}

// Head returns the current head Commit, which contains the current root of the DataStore's value tree.
func (ds *DataStore) Head() Commit {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

// Commit updates the commit that a datastore points at. The new Commit is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns 'false' and the current snapshot of the datastore so that the client can merge the changes and try again.
func (ds *DataStore) Commit(v types.Value) (DataStore, bool) {
	p := NewSetOfCommit()
	if head, ok := ds.MaybeHead(); ok {
		p = p.Insert(head)
	}
	return ds.CommitWithParents(v, p)
}

// CommitWithParents updates the commit that a datastore points at. The new Commit is constructed using v and p.
// If the update cannot be performed, e.g., because of a conflict, CommitWithParents returns 'false' and the current snapshot of the datastore so that the client can merge the changes and try again.
func (ds *DataStore) CommitWithParents(v types.Value, p SetOfCommit) (DataStore, bool) {
	ok := ds.doCommit(NewCommit().SetParents(p.NomsValue()).SetValue(v))
	return newDataStoreInternal(ds.ChunkStore), ok
}

// doCommit manages concurrent access the single logical piece of mutable state: the current head. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *DataStore) doCommit(commit Commit) bool {
	currentRootRef := ds.Root()

	// Note: |currentHead| may be different from ds.head and *must* be consistent with currentRootRef.
	// If ds.head is nil, then any commit is allowed.
	if ds.head != nil {
		var currentHead Commit
		if currentRootRef == ds.head.Ref() {
			currentHead = *ds.head
		} else {
			currentHead = *commitFromRef(currentRootRef, ds)
		}

		// Allow only fast-forward commits.
		if commit.Equals(currentHead) {
			return true
		} else if !descendsFrom(commit, currentHead) {
			return false
		}
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

type Flags struct {
	cflags chunks.Flags
	hflags http.Flags
}

func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		chunks.NewFlagsWithPrefix(prefix),
		http.NewFlagsWithPrefix(prefix),
	}
}

func (f Flags) CreateDataStore() (DataStore, bool) {
	cs := f.cflags.CreateStore()
	if cs == nil {
		cs = f.hflags.CreateClient()
		if cs == nil {
			return DataStore{}, false
		}
	}

	ds := NewDataStore(cs)
	return ds, true
}
