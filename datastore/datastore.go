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
}

func (ds *DataStore) GetRoots() types.Set {
	rootRef := ds.chunks.Root()
	if (rootRef == ref.Ref{}) {
		return types.NewSet()
	}

	return enc.MustReadValue(rootRef, ds.chunks).(types.Set)
}

func (ds *DataStore) Commit(newRoots types.Set) {
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
}

func (ds *DataStore) doCommit(add, remove types.Set) bool {
	oldRoot := ds.chunks.Root()
	oldRoots := ds.GetRoots()

	prexisting := make([]types.Value, 0)
	ds.rc.Update(oldRoot)
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
	newRef, err := enc.WriteValue(newRoots, ds.chunks)
	Chk.NoError(err)

	return ds.chunks.UpdateRoot(newRef, oldRoot)
}
