package commit

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

type Reachable interface {
	IsSupercededFrom(candidate, root ref.Ref) bool
}

type Commit struct {
	root      store.RootStore
	source    store.ChunkSource
	sink      store.ChunkSink
	reachable Reachable
}

func (c *Commit) GetRoots() (currentRoots types.Set) {
	rootRef := c.root.Root()
	if (rootRef == ref.Ref{}) {
		return types.NewSet()
	}

	return enc.MustReadValue(rootRef, c.source).(types.Set)
}

func (c *Commit) Commit(newRoots types.Set) {
	Chk.True(newRoots.Len() > 0)

	parentsList := make([]types.Set, newRoots.Len())
	i := uint64(0)
	newRoots.Iter(func(root types.Value) (stop bool) {
		parentsList[i] = root.(types.Map).Get(types.NewString("parents")).(types.Set)
		i++
		return false
	})

	superceded := types.NewSet().Union(parentsList...)
	for !c.doCommit(newRoots, superceded) {
	}
}

func (c *Commit) doCommit(add, remove types.Set) bool {
	oldRoot := c.root.Root()
	oldRoots := c.GetRoots()

	prexisting := make([]types.Value, 0)
	add.Iter(func(r types.Value) (stop bool) {
		if c.reachable.IsSupercededFrom(r.Ref(), oldRoot) {
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
	newRef, err := enc.WriteValue(newRoots, c.sink)
	Chk.NoError(err)

	return c.root.UpdateRoot(newRef, oldRoot)
}
