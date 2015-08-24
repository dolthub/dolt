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

	_, ok := ds.MaybeHead()
	assert.False(ok)

	// |a|
	a := types.NewString("a")
	ds2, ok := ds.Commit(a)
	assert.True(ok)

	// The old datastore still still has no head.
	_, ok = ds.MaybeHead()
	assert.False(ok)

	// The new datastore has |a|.
	aCommit := ds2.Head()
	assert.True(aCommit.Value().Equals(a))
	ds = ds2

	// |a| <- |b|
	b := types.NewString("b")
	ds, ok = ds.Commit(b)
	assert.True(ok)
	assert.True(ds.Head().Value().Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.NewString("c")
	ds, ok = ds.CommitWithParents(c, NewSetOfCommit().Insert(aCommit))
	assert.False(ok)
	assert.True(ds.Head().Value().Equals(b))

	// |a| <- |b| <- |d|
	d := types.NewString("d")
	ds, ok = ds.Commit(d)
	assert.True(ok)
	assert.True(ds.Head().Value().Equals(d))

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	ds, ok = ds.CommitWithParents(b, NewSetOfCommit().Insert(aCommit))
	assert.False(ok)
	assert.True(ds.Head().Value().Equals(d))
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
	a := types.NewString("a")
	ds, ok := ds.Commit(a)
	b := types.NewString("b")
	ds, ok = ds.Commit(b)
	assert.True(ok)
	assert.True(ds.Head().Value().Equals(b))

	// Important to create this here.
	ds2 := NewDataStore(chunks)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.NewString("c")
	ds, ok = ds.Commit(c)
	assert.True(ok)
	assert.True(ds.Head().Value().Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, DataStore returned by Commit() should have |c| as Head.
	e := types.NewString("e")
	ds2, ok = ds2.Commit(e)
	assert.False(ok)
	assert.True(ds.Head().Value().Equals(c))
}
