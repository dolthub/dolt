package commit

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

type Commit struct {
	root   store.RootStore
	source store.ChunkSource
	sink   store.ChunkSink
}

func (c *Commit) GetRoots() (currentRoots types.Set) {
	rootRef := c.root.Root()
	if (rootRef == ref.Ref{}) {
		return types.NewSet()
	}

	rootSetValue, err := enc.ReadValue(rootRef, c.source)
	Chk.NoError(err)
	return rootSetValue.(types.Set)
}

func (c *Commit) doCommit(add, remove types.Set) bool {
	oldRoots := c.GetRoots()
	oldRef := oldRoots.Ref()
	if oldRoots.Len() == 0 {
		oldRef = ref.Ref{}
	}

	newRoots := oldRoots.Subtract(remove)
	newRoots = newRoots.Union(add)

	// TODO(rafael): This set will be orphaned if this UpdateRoot below fails
	newRef, err := enc.WriteValue(newRoots, c.sink)
	Chk.NoError(err)

	return c.root.UpdateRoot(newRef, oldRef)
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
