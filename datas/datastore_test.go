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

	commit := ds.Head()
	assert.True(commit.Equals(EmptyCommit))

	// |a|
	a := NewCommit().SetParents(makeSetValue(commit)).SetValue(types.NewString("a"))
	ds2, ok := ds.Commit(a)
	assert.True(ok)

	// The old datastore still still references |commit|.
	assert.True(ds.Head().Equals(commit))

	// The new datastore has |a|.
	assert.True(ds2.Head().Equals(a))
	ds = ds2

	// |a| <- |b|
	b := NewCommit().SetParents(makeSetValue(a)).SetValue(types.NewString("b"))
	ds, ok = ds.Commit(b)
	assert.True(ok)
	assert.True(ds.Head().Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := NewCommit().SetParents(makeSetValue(a)).SetValue(types.NewString("c"))
	ds, ok = ds.Commit(c)
	assert.False(ok)
	assert.True(ds.Head().Equals(b))

	// |a| <- |b| <- |d|
	d := NewCommit().SetParents(makeSetValue(b)).SetValue(types.NewString("d"))
	ds, ok = ds.Commit(d)
	assert.True(ok)
	assert.True(ds.Head().Equals(d))

	// Attempt to recommit |b|
	// Should be disallowed.
	ds, ok = ds.Commit(b)
	assert.False(ok)
	assert.True(ds.Head().Equals(d))
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
	a := NewCommit().SetParents(makeSetValue(ds.Head())).SetValue(types.NewString("a"))
	ds, ok := ds.Commit(a)
	b := NewCommit().SetParents(makeSetValue(a)).SetValue(types.NewString("b"))
	ds, ok = ds.Commit(b)

	// Important to create this here.
	ds2 := NewDataStore(chunks)

	// Change 1:
	// |a| <- |b| <- |c|
	c := NewCommit().SetParents(makeSetValue(b)).SetValue(types.NewString("c"))
	ds, ok = ds.Commit(c)
	assert.True(ok)
	assert.True(ds.Head().Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, DataStore returned by Commit() should have |c| as Head.
	e := NewCommit().SetParents(makeSetValue(b)).SetValue(types.NewString("e"))
	ds2, ok = ds2.Commit(e)
	assert.False(ok)
	assert.True(ds.Head().Equals(c))
}

func makeSetValue(commit Commit) types.Set {
	return NewSetOfCommit().Insert(commit).NomsValue()
}
