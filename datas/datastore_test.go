package datas

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
)

func TestDataStoreCommit(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID := "ds1"

	datasets := ds.Datasets()
	assert.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit(cs).SetValue(a)
	ds2, ok := ds.Commit(datasetID, aCommit)
	assert.True(ok)

	// The old datastore still still has no head.
	_, ok = ds.MaybeHead(datasetID)
	assert.False(ok)

	// The new datastore has |a|.
	aCommit1 := ds2.Head(datasetID)
	assert.True(aCommit1.Value().Equals(a))
	ds = ds2

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit(cs).SetValue(b).SetParents(NewSetOfRefOfCommit(cs).Insert(NewRefOfCommit(aCommit.Ref())))
	ds, ok = ds.Commit(datasetID, bCommit)
	assert.True(ok)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.NewString("c")
	cCommit := NewCommit(cs).SetValue(c)
	ds, ok = ds.Commit(datasetID, cCommit)
	assert.False(ok)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// |a| <- |b| <- |d|
	d := types.NewString("d")
	dCommit := NewCommit(cs).SetValue(d).SetParents(NewSetOfRefOfCommit(cs).Insert(NewRefOfCommit(bCommit.Ref())))
	ds, ok = ds.Commit(datasetID, dCommit)
	assert.True(ok)
	assert.True(ds.Head(datasetID).Value().Equals(d))

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	ds, ok = ds.Commit(datasetID, bCommit)
	assert.False(ok)
	assert.True(ds.Head(datasetID).Value().Equals(d))

	// Add a commit to a different datasetId
	_, ok = ds.Commit("otherDs", aCommit)
	assert.True(ok)

	// Get a fresh datastore, and verify that both datasets are present
	newDs := NewDataStore(cs)
	datasets2 := newDs.Datasets()
	assert.Equal(uint64(2), datasets2.Len())
}

func TestDataStoreConcurrency(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID := "ds1"

	// Setup:
	// |a| <- |b|
	a := types.NewString("a")
	aCommit := NewCommit(cs).SetValue(a)
	ds, ok := ds.Commit(datasetID, aCommit)
	b := types.NewString("b")
	bCommit := NewCommit(cs).SetValue(b).SetParents(NewSetOfRefOfCommit(cs).Insert(NewRefOfCommit(aCommit.Ref())))
	ds, ok = ds.Commit(datasetID, bCommit)
	assert.True(ok)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// Important to create this here.
	ds2 := NewDataStore(cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.NewString("c")
	cCommit := NewCommit(cs).SetValue(c).SetParents(NewSetOfRefOfCommit(cs).Insert(NewRefOfCommit(bCommit.Ref())))
	ds, ok = ds.Commit(datasetID, cCommit)
	assert.True(ok)
	assert.True(ds.Head(datasetID).Value().Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, DataStore returned by Commit() should have |c| as Head.
	e := types.NewString("e")
	eCommit := NewCommit(cs).SetValue(e).SetParents(NewSetOfRefOfCommit(cs).Insert(NewRefOfCommit(bCommit.Ref())))
	ds2, ok = ds2.Commit(datasetID, eCommit)
	assert.False(ok)
	assert.True(ds.Head(datasetID).Value().Equals(c))
}
