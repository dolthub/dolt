package datastore

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDataStoreCommit(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	chunks := chunks.NewFileStore(dir, "root")
	ds := NewDataStore(chunks, chunks)

	roots := ds.Roots()
	assert.Equal(uint64(0), roots.Len())

	// |a|
	a := types.NewMap(
		types.NewString("parents"), roots,
		types.NewString("value"), types.NewString("a"),
	)
	aSet := types.NewSet(a)
	ds2 := ds.Commit(aSet)

	// The old datastore still still references the old roots.
	assert.True(ds.Roots().Equals(roots))

	// The new datastore has the new roots.
	assert.True(ds2.Roots().Equals(aSet))
	ds = ds2

	// |a| <- |b|
	b := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("b"),
	)
	bSet := types.NewSet(b)
	ds = ds.Commit(bSet)
	assert.True(ds.Roots().Equals(bSet))

	// |a| <- |b|
	//   \----|c|
	c := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("c"),
	)
	cSet := types.NewSet(c)
	ds = ds.Commit(cSet)
	bcSet := bSet.Insert(c)
	assert.True(ds.Roots().Equals(bcSet))

	// |a| <- |b|
	//   \----|c|
	//    \---|d|
	d := types.NewMap(
		types.NewString("parents"), aSet,
		types.NewString("value"), types.NewString("d"),
	)
	dSet := types.NewSet(d)
	enc.WriteValue(dSet, chunks)

	ds = ds.Commit(dSet)
	bcdSet := bcSet.Insert(d)
	assert.True(ds.Roots().Equals(bcdSet))

	// |a| <- |b| <-- |e|
	//   \----|c| <--/
	//    \---|d|
	e := types.NewMap(
		types.NewString("parents"), bcSet,
		types.NewString("value"), types.NewString("e"),
	)
	eSet := types.NewSet(e)
	ds = ds.Commit(eSet)
	deSet := dSet.Insert(e)
	assert.True(ds.Roots().Equals(deSet))

	// |a| <- |b| <-- |e| <- |f|
	//   \----|c| <--/      /
	//    \---|d| <-------/
	f := types.NewMap(
		types.NewString("parents"), deSet,
		types.NewString("value"), types.NewString("f"),
	)

	fSet := types.NewSet(f)
	ds = ds.Commit(fSet)
	assert.True(ds.Roots().Equals(fSet))

	// Attempt to recommit |b|
	ds = ds.Commit(bSet)
	assert.True(ds.Roots().Equals(fSet))

	// Attempt to recommit |f|
	ds = ds.Commit(fSet)
	assert.True(ds.Roots().Equals(fSet))

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

	ds = ds.Commit(gdSet)
	assert.True(ds.Roots().Equals(gSet))

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

	ds = ds.Commit(hSet)
	hgSet := hSet.Insert(g)
	assert.True(ds.Roots().Equals(hgSet))
}
