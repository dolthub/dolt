package commit

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestCommit(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	store := store.NewFileStore(dir, "root")
	commit := &Commit{store, store, store}

	roots := commit.GetRoots()
	assert.Equal(uint64(0), roots.Len())

	// |a|
	a := types.NewMap(
		types.NewString("parents"), roots,
		types.NewString("value"), types.NewString("a"),
	)
	aSet := types.NewSet(a)
	enc.WriteValue(aSet, store)
	commit.Commit(aSet)
	assert.Equal(commit.GetRoots(), aSet)

	// |a| <- |b|
	b := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("b"),
	)
	bSet := types.NewSet(b)
	enc.WriteValue(bSet, store)
	commit.Commit(bSet)
	assert.Equal(commit.GetRoots(), bSet)

	// |a| <- |b|
	//   \----|c|
	c := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("c"),
	)
	cSet := types.NewSet(c)
	enc.WriteValue(cSet, store)
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
	enc.WriteValue(eSet, store)
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
	enc.WriteValue(fSet, store)
	commit.Commit(fSet)
	assert.Equal(commit.GetRoots(), fSet)
}
