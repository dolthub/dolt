package datas

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
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
	a := NewRoot().SetParents(roots.NomsValue()).SetValue(types.NewString("a"))
	aSet := NewRootSet().Insert(a)
	ds2 := ds.Commit(aSet)

	// The old datastore still still references the old roots.
	assert.True(ds.Roots().Equals(roots))

	// The new datastore has the new roots.
	assert.True(ds2.Roots().Equals(aSet))
	ds = ds2

	// |a| <- |b|
	b := NewRoot().SetParents(aSet.NomsValue()).SetValue(types.NewString("b"))
	bSet := NewRootSet().Insert(b)
	ds = ds.Commit(bSet)
	assert.True(ds.Roots().Equals(bSet))

	// |a| <- |b|
	//   \----|c|
	c := NewRoot().SetParents(aSet.NomsValue()).SetValue(types.NewString("c"))
	cSet := NewRootSet().Insert(c)
	ds = ds.Commit(cSet)
	bcSet := bSet.Insert(c)
	assert.True(ds.Roots().Equals(bcSet))

	// |a| <- |b|
	//   \----|c|
	//    \---|d|
	d := NewRoot().SetParents(aSet.NomsValue()).SetValue(types.NewString("d"))
	dSet := NewRootSet().Insert(d)
	types.WriteValue(dSet.NomsValue(), chunks)

	ds = ds.Commit(dSet)
	bcdSet := bcSet.Insert(d)
	assert.True(ds.Roots().Equals(bcdSet))

	// |a| <- |b| <-- |e|
	//   \----|c| <--/
	//    \---|d|
	e := NewRoot().SetParents(bcSet.NomsValue()).SetValue(types.NewString("e"))
	eSet := NewRootSet().Insert(e)
	ds = ds.Commit(eSet)
	deSet := dSet.Insert(e)
	assert.True(ds.Roots().Equals(deSet))

	// |a| <- |b| <-- |e| <- |f|
	//   \----|c| <--/      /
	//    \---|d| <-------/
	f := NewRoot().SetParents(deSet.NomsValue()).SetValue(types.NewString("f"))
	fSet := NewRootSet().Insert(f)
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
	g := NewRoot().SetParents(fdSet.NomsValue()).SetValue(types.NewString("g"))
	gSet := NewRootSet().Insert(g)
	gdSet := gSet.Insert(c)

	ds = ds.Commit(gdSet)
	assert.True(ds.Roots().Equals(gSet))

	//      / -|h|
	//    /    |
	// |a| <- |b| <-- |e| <- |f| <- |g|
	//   \----|c| <--/      /      /
	//    \---|d| <-------/------/
	abSet := aSet.Insert(b)
	h := NewRoot().SetParents(abSet.NomsValue()).SetValue(types.NewString("h"))
	hSet := NewRootSet().Insert(h)

	ds = ds.Commit(hSet)
	hgSet := hSet.Insert(g)
	assert.True(ds.Roots().Equals(hgSet))
}
