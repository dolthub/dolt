package datastore

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type DataStore struct {
	chunks.ChunkStore

	rt    chunks.RootTracker
	rc    *rootCache
	roots types.Set
}

func NewDataStore(cs chunks.ChunkStore, rt chunks.RootTracker) DataStore {
	return NewDataStoreWithCache(cs, rt, NewRootCache(cs))
}

func NewDataStoreWithCache(cs chunks.ChunkStore, rt chunks.RootTracker, rc *rootCache) DataStore {
	var roots types.Set
	rootRef := rt.Root()
	if (rootRef == ref.Ref{}) {
		roots = types.NewSet()
	} else {
		// BUG 11: This reads the entire database into memory. Whoopsie.
		roots = enc.MustReadValue(rootRef, cs).(types.Set)
	}
	return DataStore{
		cs, rt, rc, roots,
	}
}

func (ds *DataStore) Roots() types.Set {
	return ds.roots
}

func (ds *DataStore) Commit(newRoots types.Set) DataStore {
	Chk.True(newRoots.Len() > 0)

	parentsList := make([]types.Set, newRoots.Len())
	i := uint64(0)
	newRoots.Iter(func(root types.Value) (stop bool) {
		parentsList[i] = root.(types.Map).Get(types.NewString("parents")).(types.Set)
		i++
		return false
	})

	superceded := types.NewSet().Union(parentsList...)
	for !ds.doCommit(newRoots, superceded) {
	}

	return NewDataStoreWithCache(ds.ChunkStore, ds.rt, ds.rc)
}

func (ds *DataStore) doCommit(add, remove types.Set) bool {
	oldRootRef := ds.rt.Root()
	oldRoots := ds.Roots()

	prexisting := make([]types.Value, 0)
	ds.rc.Update(oldRootRef)
	add.Iter(func(r types.Value) (stop bool) {
		if ds.rc.Contains(r.Ref()) {
			prexisting = append(prexisting, r)
		}
		return false
	})
	add = add.Remove(prexisting...)
	if add.Len() == 0 {
		return true
	}

	newRoots := oldRoots.Subtract(remove).Union(add)

	// TODO(rafael): This set will be orphaned if this UpdateRoot below fails
	newRootRef, err := enc.WriteValue(newRoots, ds)
	Chk.NoError(err)

	return ds.rt.UpdateRoot(newRootRef, oldRootRef)
}
