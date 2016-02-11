package datas

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDataStoreAccess(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	input := "abc"

	c := chunks.NewChunk([]byte(input))
	c1 := ds.Get(c.Ref())
	assert.True(c1.IsEmpty())

	has := ds.Has(c.Ref())
	assert.False(has)

	ds.Put(c)
	c1 = ds.Get(c.Ref())
	assert.False(c1.IsEmpty())

	has = ds.Has(c.Ref())
	assert.True(has)
}

func TestDataStoreCommit(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID := "ds1"

	datasets := ds.Datasets()
	assert.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().SetValue(a)
	ds2, err := ds.Commit(datasetID, aCommit)
	assert.NoError(err)

	// The old datastore still still has no head.
	_, ok := ds.MaybeHead(datasetID)
	assert.False(ok)

	// The new datastore has |a|.
	aCommit1 := ds2.Head(datasetID)
	assert.True(aCommit1.Value().Equals(a))
	ds = ds2

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().SetValue(b).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(aCommit.Ref())))
	ds, err = ds.Commit(datasetID, bCommit)
	assert.NoError(err)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.NewString("c")
	cCommit := NewCommit().SetValue(c)
	ds, err = ds.Commit(datasetID, cCommit)
	assert.Error(err)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// |a| <- |b| <- |d|
	d := types.NewString("d")
	dCommit := NewCommit().SetValue(d).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(bCommit.Ref())))
	ds, err = ds.Commit(datasetID, dCommit)
	assert.NoError(err)
	assert.True(ds.Head(datasetID).Value().Equals(d))

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	ds, err = ds.Commit(datasetID, bCommit)
	assert.Error(err)
	assert.True(ds.Head(datasetID).Value().Equals(d))

	// Add a commit to a different datasetId
	_, err = ds.Commit("otherDs", aCommit)
	assert.NoError(err)

	// Get a fresh datastore, and verify that both datasets are present
	newDs := NewDataStore(cs)
	datasets2 := newDs.Datasets()
	assert.Equal(uint64(2), datasets2.Len())
}

func TestDataStoreDelete(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID1, datasetID2 := "ds1", "ds2"

	datasets := ds.Datasets()
	assert.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	ds, err := ds.Commit(datasetID1, NewCommit().SetValue(a))
	assert.NoError(err)
	assert.True(ds.Head(datasetID1).Value().Equals(a))

	// ds1; |a|, ds2: |b|
	b := types.NewString("b")
	ds, err = ds.Commit(datasetID2, NewCommit().SetValue(b))
	assert.NoError(err)
	assert.True(ds.Head(datasetID2).Value().Equals(b))

	ds, err = ds.Delete(datasetID1)
	assert.NoError(err)
	assert.True(ds.Head(datasetID2).Value().Equals(b))
	h, present := ds.MaybeHead(datasetID1)
	assert.False(present, "Dataset %s should not be present, but head is %v", datasetID1, h.Value())

	// Get a fresh datastore, and verify that only ds1 is present
	newDs := NewDataStore(cs)
	datasets = newDs.Datasets()
	assert.Equal(uint64(1), datasets.Len())
	_, present = ds.MaybeHead(datasetID2)
	assert.True(present, "Dataset %s should be present", datasetID2)
}

func TestDataStoreDeleteConcurrent(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID := "ds1"

	datasets := ds.Datasets()
	assert.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().SetValue(a)
	ds, err := ds.Commit(datasetID, aCommit)
	assert.NoError(err)

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().SetValue(b).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(aCommit.Ref())))
	ds2, err := ds.Commit(datasetID, bCommit)
	assert.NoError(err)
	assert.True(ds.Head(datasetID).Value().Equals(a))
	assert.True(ds2.Head(datasetID).Value().Equals(b))

	ds, err = ds.Delete(datasetID)
	assert.NoError(err)
	h, present := ds.MaybeHead(datasetID)
	assert.False(present, "Dataset %s should not be present, but head is %v", datasetID, h.Value())
	h, present = ds2.MaybeHead(datasetID)
	assert.True(present, "Dataset %s should be present", datasetID)

	// Get a fresh datastore, and verify that no datastores are present
	newDs := NewDataStore(cs)
	datasets = newDs.Datasets()
	assert.Equal(uint64(0), datasets.Len())
}

func TestDataStoreConcurrency(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	ds := NewDataStore(cs)
	datasetID := "ds1"

	// Setup:
	// |a| <- |b|
	a := types.NewString("a")
	aCommit := NewCommit().SetValue(a)
	ds, err := ds.Commit(datasetID, aCommit)
	b := types.NewString("b")
	bCommit := NewCommit().SetValue(b).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(aCommit.Ref())))
	ds, err = ds.Commit(datasetID, bCommit)
	assert.NoError(err)
	assert.True(ds.Head(datasetID).Value().Equals(b))

	// Important to create this here.
	ds2 := NewDataStore(cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.NewString("c")
	cCommit := NewCommit().SetValue(c).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(bCommit.Ref())))
	ds, err = ds.Commit(datasetID, cCommit)
	assert.NoError(err)
	assert.True(ds.Head(datasetID).Value().Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, DataStore returned by Commit() should have |c| as Head.
	e := types.NewString("e")
	eCommit := NewCommit().SetValue(e).SetParents(NewSetOfRefOfCommit().Insert(NewRefOfCommit(bCommit.Ref())))
	ds2, err = ds2.Commit(datasetID, eCommit)
	assert.Error(err)
	assert.True(ds.Head(datasetID).Value().Equals(c))
}
