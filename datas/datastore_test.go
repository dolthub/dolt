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
	ds := NewDataStore(chunks)

	commits := ds.Heads()
	assert.Equal(uint64(0), commits.Len())

	// |a|
	a := NewCommit().SetParents(commits.NomsValue()).SetValue(types.NewString("a"))
	aSet := NewSetOfCommit().Insert(a)
	ds2 := ds.Commit(aSet)

	// The old datastore still still references the old commits.
	assert.True(ds.Heads().Equals(commits))

	// The new datastore has the new commits.
	assert.True(ds2.Heads().Equals(aSet))
	ds = ds2

	// |a| <- |b|
	b := NewCommit().SetParents(aSet.NomsValue()).SetValue(types.NewString("b"))
	bSet := NewSetOfCommit().Insert(b)
	ds = ds.Commit(bSet)
	assert.True(ds.Heads().Equals(bSet))

	// |a| <- |b|
	//   \----|c|
	c := NewCommit().SetParents(aSet.NomsValue()).SetValue(types.NewString("c"))
	cSet := NewSetOfCommit().Insert(c)
	ds = ds.Commit(cSet)
	bcSet := bSet.Insert(c)
	assert.True(ds.Heads().Equals(bcSet))

	// |a| <- |b|
	//   \----|c|
	//    \---|d|
	d := NewCommit().SetParents(aSet.NomsValue()).SetValue(types.NewString("d"))
	dSet := NewSetOfCommit().Insert(d)
	types.WriteValue(dSet.NomsValue(), chunks)

	ds = ds.Commit(dSet)
	bcdSet := bcSet.Insert(d)
	assert.True(ds.Heads().Equals(bcdSet))

	// |a| <- |b| <-- |e|
	//   \----|c| <--/
	//    \---|d|
	e := NewCommit().SetParents(bcSet.NomsValue()).SetValue(types.NewString("e"))
	eSet := NewSetOfCommit().Insert(e)
	ds = ds.Commit(eSet)
	deSet := dSet.Insert(e)
	assert.True(ds.Heads().Equals(deSet))

	// |a| <- |b| <-- |e| <- |f|
	//   \----|c| <--/      /
	//    \---|d| <-------/
	f := NewCommit().SetParents(deSet.NomsValue()).SetValue(types.NewString("f"))
	fSet := NewSetOfCommit().Insert(f)
	ds = ds.Commit(fSet)
	assert.True(ds.Heads().Equals(fSet))

	// Attempt to recommit |b|
	ds = ds.Commit(bSet)
	assert.True(ds.Heads().Equals(fSet))

	// Attempt to recommit |f|
	ds = ds.Commit(fSet)
	assert.True(ds.Heads().Equals(fSet))

	// Attempt to recommit |c| while committing |g|
	// |a| <- |b| <-- |e| <- |f| <- |g|
	//   \----|c| <--/      /      /
	//    \---|d| <-------/------/
	fdSet := fSet.Insert(d)
	g := NewCommit().SetParents(fdSet.NomsValue()).SetValue(types.NewString("g"))
	gSet := NewSetOfCommit().Insert(g)
	gdSet := gSet.Insert(c)

	ds = ds.Commit(gdSet)
	assert.True(ds.Heads().Equals(gSet))

	//      / -|h|
	//    /    |
	// |a| <- |b| <-- |e| <- |f| <- |g|
	//   \----|c| <--/      /      /
	//    \---|d| <-------/------/
	abSet := aSet.Insert(b)
	h := NewCommit().SetParents(abSet.NomsValue()).SetValue(types.NewString("h"))
	hSet := NewSetOfCommit().Insert(h)

	ds = ds.Commit(hSet)
	hgSet := hSet.Insert(g)
	assert.True(ds.Heads().Equals(hgSet))
}

func TestDataStoreConcurrency(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	chunks := chunks.NewFileStore(dir, "commit")
	ds := NewDataStore(chunks)

	// Setup:
	// |a| <- |b|
	//   \----|c|
	a := NewCommit().SetParents(ds.Heads().NomsValue()).SetValue(types.NewString("a"))
	aSet := NewSetOfCommit().Insert(a)
	ds = ds.Commit(aSet)
	b := NewCommit().SetParents(aSet.NomsValue()).SetValue(types.NewString("b"))
	bSet := NewSetOfCommit().Insert(b)
	ds = ds.Commit(bSet)
	c := NewCommit().SetParents(aSet.NomsValue()).SetValue(types.NewString("c"))
	cSet := NewSetOfCommit().Insert(c)
	ds = ds.Commit(cSet)
	bcSet := bSet.Insert(c)

	// Important to create this here.
	ds2 := NewDataStore(chunks)

	// Change 1:
	// |a| <- |b| <- |d|
	//   \----|c| --/
	d := NewCommit().SetParents(bcSet.NomsValue()).SetValue(types.NewString("d"))
	dSet := NewSetOfCommit().Insert(d)
	types.WriteValue(dSet.NomsValue(), chunks)
	ds = ds.Commit(dSet)

	// Change 2:
	// |a| <- |b| <- |e|
	//   \----|c| --/
	e := NewCommit().SetParents(bcSet.NomsValue()).SetValue(types.NewString("e"))
	eSet := NewSetOfCommit().Insert(e)
	types.WriteValue(eSet.NomsValue(), chunks)
	ds2 = ds2.Commit(eSet)

	// The chunkstore should have tracked that two conflicting commits happened and both |d| and |e| are now commits
	deSet := dSet.Insert(e)
	finalCommits := SetOfCommitFromVal(types.MustReadValue(chunks.Root(), chunks).(types.Set))
	assert.True(finalCommits.Equals(deSet))
}
