package dataset

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDatasetCommitTracker(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	cs := chunks.NewMemoryStore()

	ds1 := NewDataset(datas.NewDataStore(cs), id1)
	ds1Commit := types.NewString("Commit value for " + id1)
	ds1, err := ds1.Commit(ds1Commit)
	assert.NoError(err)

	ds2 := NewDataset(datas.NewDataStore(cs), id2)
	ds2Commit := types.NewString("Commit value for " + id2)
	ds2, err = ds2.Commit(ds2Commit)
	assert.NoError(err)

	assert.EqualValues(ds1Commit, ds1.Head().Value())
	assert.EqualValues(ds2Commit, ds2.Head().Value())
	assert.False(ds2.Head().Value().Equals(ds1Commit))
	assert.False(ds1.Head().Value().Equals(ds2Commit))

	assert.Equal("sha1-6ddf39e2ccd452d06e610713e0261cd9b31d5681", cs.Root().String())
}

func newDS(id string, cs *chunks.MemoryStore) Dataset {
	store := datas.NewDataStore(cs)
	return NewDataset(store, id)
}

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	cs := chunks.NewMemoryStore()

	ds1 := newDS(id1, cs)

	// ds1: |a|
	a := types.NewString("a")
	ds1, err := ds1.Commit(a)
	assert.NoError(err)
	assert.True(ds1.Head().Value().Equals(a))

	// ds1: |a|
	//        \ds2
	ds2 := newDS(id2, cs)
	ds2, err = ds2.Commit(ds1.Head().Value())
	assert.NoError(err)
	assert.True(ds2.Head().Value().Equals(a))

	// ds1: |a| <- |b|
	b := types.NewString("b")
	ds1, err = ds1.Commit(b)
	assert.NoError(err)
	assert.True(ds1.Head().Value().Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.NewString("c")
	ds2, err = ds2.Commit(c)
	assert.NoError(err)
	assert.True(ds2.Head().Value().Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := datas.NewSetOfRefOfCommit().
		Insert(types.NewTypedRefFromValue(ds1.Head())).
		Insert(types.NewTypedRefFromValue(ds2.Head()))
	d := types.NewString("d")
	ds2, err = ds2.CommitWithParents(d, mergeParents)
	assert.NoError(err)
	assert.True(ds2.Head().Value().Equals(d))

	ds1, err = ds1.CommitWithParents(d, mergeParents)
	assert.NoError(err)
	assert.True(ds1.Head().Value().Equals(d))
}

func TestTwoClientsWithEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	cs := chunks.NewMemoryStore()

	dsx := newDS(id1, cs)
	dsy := newDS(id1, cs)

	// dsx: || -> |a|
	a := types.NewString("a")
	dsx, err := dsx.Commit(a)
	assert.NoError(err)
	assert.True(dsx.Head().Value().Equals(a))

	// dsy: || -> |b|
	_, ok := dsy.MaybeHead()
	assert.False(ok)
	b := types.NewString("b")
	dsy, err = dsy.Commit(b)
	assert.Error(err)
	// Commit failed, but ds1 now has latest head, so we should be able to just try again.
	// dsy: |a| -> |b|
	dsy, err = dsy.Commit(b)
	assert.NoError(err)
	assert.True(dsy.Head().Value().Equals(b))
}

func TestTwoClientsWithNonEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	cs := chunks.NewMemoryStore()

	a := types.NewString("a")
	{
		// ds1: || -> |a|
		ds1 := newDS(id1, cs)
		ds1, err := ds1.Commit(a)
		assert.NoError(err)
		assert.True(ds1.Head().Value().Equals(a))
	}

	dsx := newDS(id1, cs)
	dsy := newDS(id1, cs)

	// dsx: |a| -> |b|
	assert.True(dsx.Head().Value().Equals(a))
	b := types.NewString("b")
	dsx, err := dsx.Commit(b)
	assert.NoError(err)
	assert.True(dsx.Head().Value().Equals(b))

	// dsy: |a| -> |c|
	assert.True(dsy.Head().Value().Equals(a))
	c := types.NewString("c")
	dsy, err = dsy.Commit(c)
	assert.Error(err)
	assert.True(dsy.Head().Value().Equals(b))
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, err = dsy.Commit(c)
	assert.NoError(err)
	assert.True(dsy.Head().Value().Equals(c))
}
