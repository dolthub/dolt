package datastore

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type DataStore struct {
	chunks chunks.ChunkStore
	rc     *rootCache
	roots  types.Set
}

func NewDataStore(cs chunks.ChunkStore) DataStore {
	return NewDataStoreWithCache(cs, NewRootCache(cs))
}

func NewDataStoreWithCache(cs chunks.ChunkStore, rc *rootCache) DataStore {
	var roots types.Set
	rootRef := cs.Root()
	if (rootRef == ref.Ref{}) {
		roots = types.NewSet()
	} else {
		roots = enc.MustReadValue(rootRef, cs).(types.Set)
	}
	return DataStore{
		cs, rc, roots,
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

	return NewDataStoreWithCache(ds.chunks, ds.rc)
}

func (ds *DataStore) doCommit(add, remove types.Set) bool {
	oldRootRef := ds.chunks.Root()
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
	newRootRef, err := enc.WriteValue(newRoots, ds.chunks)
	Chk.NoError(err)

	return ds.chunks.UpdateRoot(newRootRef, oldRootRef)
}
