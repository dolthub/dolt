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
	rc    *rootCache
	roots RootSet
}

func NewDataStore(cs chunks.ChunkStore, rt chunks.RootTracker) DataStore {
	return newDataStoreInternal(cs, rt, NewRootCache(cs))
}

func newDataStoreInternal(cs chunks.ChunkStore, rt chunks.RootTracker, rc *rootCache) DataStore {
	return DataStore{
		cs, rt, rc, rootSetFromRef(rt.Root(), cs),
	}
}

func rootSetFromRef(rootRef ref.Ref, cs chunks.ChunkSource) RootSet {
	var roots RootSet
	if (rootRef == ref.Ref{}) {
		roots = NewRootSet()
	} else {
		roots = RootSetFromVal(types.MustReadValue(rootRef, cs).(types.Set))
	}

	return roots
}

func (ds *DataStore) Roots() RootSet {
	return ds.roots
}

func (ds *DataStore) Commit(newRoots RootSet) DataStore {
	Chk.True(newRoots.Len() > 0)

	parentsList := make([]types.Set, newRoots.Len())
	i := uint64(0)
	newRoots.Iter(func(root Root) (stop bool) {
		parentsList[i] = root.Parents()
		i++
		return
	})

	superceded := types.NewSet().Union(parentsList...)
	for !ds.doCommit(newRoots, RootSet{superceded}) {
	}

	return newDataStoreInternal(ds.ChunkStore, ds.rt, ds.rc)
}

func (ds *DataStore) doCommit(add, remove RootSet) bool {
	// Note that |oldRoots| may be different from |ds.Roots| if someone else has commited since this Datastore was created. This computation must be based on the *current root* not the root associated with this Datastore.
	currentRootRef := ds.rt.Root()
	oldRoots := rootSetFromRef(currentRootRef, ds)

	prexisting := make([]Root, 0)
	ds.rc.Update(currentRootRef)
	add.Iter(func(r Root) (stop bool) {
		if ds.rc.Contains(r.Ref()) {
			prexisting = append(prexisting, r)
		}
		return
	})
	add = add.Remove(prexisting...)
	if add.Len() == 0 {
		return true
	}

	newRoots := oldRoots.Subtract(remove).Union(add)

	// TODO: This set will be orphaned if this UpdateRoot below fails
	newRootRef, err := types.WriteValue(newRoots.NomsValue(), ds)
	Chk.NoError(err)

	return ds.rt.UpdateRoot(newRootRef, currentRootRef)
}
