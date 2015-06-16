package commit

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestCommit(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	store := chunks.NewFileStore(dir, "root")
	commit := &Commit{
		store,
		NewMemCacheReachable(store),
	}

	roots := commit.GetRoots()
	assert.Equal(uint64(0), roots.Len())

	// |a|
	a := types.NewMap(
		types.NewString("parents"), roots,
		types.NewString("value"), types.NewString("a"),
	)
	aSet := types.NewSet(a)
	commit.Commit(aSet)
	assert.Equal(commit.GetRoots(), aSet)

	// |a| <- |b|
	b := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("b"),
	)
	bSet := types.NewSet(b)
	commit.Commit(bSet)
	assert.Equal(commit.GetRoots(), bSet)

	// |a| <- |b|
	//   \----|c|
	c := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("c"),
	)
	cSet := types.NewSet(c)
	commit.Commit(cSet)
	bcSet := bSet.Insert(c)
	assert.Equal(commit.GetRoots(), bcSet)

	// |a| <- |b|
	//   \----|c|
	//    \---|d|
	d := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("d"),
	)
	dSet := types.NewSet(d)
	enc.WriteValue(dSet, store)

	commit.Commit(dSet)
	bcdSet := bcSet.Insert(d)
	assert.Equal(commit.GetRoots(), bcdSet)

	// |a| <- |b| <-- |e|
	//   \----|c| <--/
	//    \---|d|
	e := types.NewMap(
		types.NewString("parents"), bcSet,
		types.NewString("value"), types.NewString("e"),
	)
	eSet := types.NewSet(e)
	commit.Commit(eSet)
	deSet := dSet.Insert(e)
	assert.Equal(commit.GetRoots(), deSet)

	// |a| <- |b| <-- |e| <- |f|
	//   \----|c| <--/      /
	//    \---|d| <-------/
	f := types.NewMap(
		types.NewString("parents"), deSet,
		types.NewString("value"), types.NewString("f"),
	)

	fSet := types.NewSet(f)
	commit.Commit(fSet)
	assert.Equal(commit.GetRoots(), fSet)

	// Attempt to recommit |b|
	commit.Commit(bSet)
	assert.Equal(commit.GetRoots(), fSet)

	// Attempt to recommit |f|
	commit.Commit(fSet)
	assert.Equal(commit.GetRoots(), fSet)

	// Attempt to recommit |c| while committing |g|
	// |a| <- |b| <-- |e| <- |f| <- |g|
	//   \----|c| <--/      /      /
	//    \---|d| <-------/------/
	fdSet := fSet.Insert(d)
	g := types.NewMap(
		types.NewString("parents"), fdSet,
		types.NewString("value"), types.NewString("g"),
	)
	gSet := types.NewSet(g)
	gdSet := gSet.Insert(c)

	commit.Commit(gdSet)
	assert.Equal(commit.GetRoots(), gSet)

	//      / -|h|
	//    /    |
	// |a| <- |b| <-- |e| <- |f| <- |g|
	//   \----|c| <--/      /      /
	//    \---|d| <-------/------/
	abSet := aSet.Insert(b)
	h := types.NewMap(
		types.NewString("parents"), abSet,
		types.NewString("value"), types.NewString("h"),
	)
	hSet := types.NewSet(h)

	commit.Commit(hSet)
	hgSet := hSet.Insert(g)
	roots = commit.GetRoots()
	assert.Equal(commit.GetRoots(), hgSet)
}
