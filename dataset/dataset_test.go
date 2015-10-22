package dataset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func TestDatasetCommitTracker(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	ms := chunks.NewMemoryStore()

	ds1 := NewDataset(datas.NewDataStore(ms), id1)
	ds1Commit := types.NewString("Commit value for " + id1)
	ds1, ok := ds1.Commit(ds1Commit)
	assert.True(ok)

	ds2 := NewDataset(datas.NewDataStore(ms), id2)
	ds2Commit := types.NewString("Commit value for " + id2)
	ds2, ok = ds2.Commit(ds2Commit)
	assert.True(ok)

	assert.EqualValues(ds1Commit, ds1.Head().Value())
	assert.EqualValues(ds2Commit, ds2.Head().Value())
	assert.False(ds2.Head().Value().Equals(ds1Commit))
	assert.False(ds1.Head().Value().Equals(ds2Commit))

	assert.Equal("sha1-079abb07bcf025f7b81f5f84feca5c74a5e008b6", ms.Root().String())
}

func newDS(id string, ms *chunks.MemoryStore) Dataset {
	store := datas.NewDataStore(ms)
	return NewDataset(store, id)
}

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	ms := chunks.NewMemoryStore()

	ds1 := newDS(id1, ms)

	// ds1: |a|
	a := types.NewString("a")
	ds1, ok := ds1.Commit(a)
	assert.True(ok)
	assert.True(ds1.Head().Value().Equals(a))

	// ds1: |a|
	//        \ds2
	ds2 := newDS(id2, ms)
	ds2, ok = ds2.Commit(ds1.Head().Value())
	assert.True(ok)
	assert.True(ds2.Head().Value().Equals(a))

	// ds1: |a| <- |b|
	b := types.NewString("b")
	ds1, ok = ds1.Commit(b)
	assert.True(ok)
	assert.True(ds1.Head().Value().Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.NewString("c")
	ds2, ok = ds2.Commit(c)
	assert.True(ok)
	assert.True(ds2.Head().Value().Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := datas.NewSetOfRefOfCommit().Insert(datas.NewRefOfCommit(ds1.Head().Ref())).Insert(datas.NewRefOfCommit(ds2.Head().Ref()))
	d := types.NewString("d")
	ds2, ok = ds2.CommitWithParents(d, mergeParents)
	assert.True(ok)
	assert.True(ds2.Head().Value().Equals(d))

	ds1, ok = ds1.CommitWithParents(d, mergeParents)
	assert.True(ok)
	assert.True(ds1.Head().Value().Equals(d))
}

func TestTwoClientsWithEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	ms := chunks.NewMemoryStore()

	dsx := newDS(id1, ms)
	dsy := newDS(id1, ms)

	// dsx: || -> |a|
	a := types.NewString("a")
	dsx, ok := dsx.Commit(a)
	assert.True(ok)
	assert.True(dsx.Head().Value().Equals(a))

	// dsy: || -> |b|
	_, ok = dsy.MaybeHead()
	assert.False(ok)
	b := types.NewString("b")
	dsy, ok = dsy.Commit(b)
	assert.False(ok)
	// Commit failed, but ds1 now has latest head, so we should be able to just try again.
	// dsy: |a| -> |b|
	dsy, ok = dsy.Commit(b)
	assert.True(ok)
	assert.True(dsy.Head().Value().Equals(b))
}

func TestTwoClientsWithNonEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	ms := chunks.NewMemoryStore()

	a := types.NewString("a")
	{
		// ds1: || -> |a|
		ds1 := newDS(id1, ms)
		ds1, ok := ds1.Commit(a)
		assert.True(ok)
		assert.True(ds1.Head().Value().Equals(a))
	}

	dsx := newDS(id1, ms)
	dsy := newDS(id1, ms)

	// dsx: |a| -> |b|
	assert.True(dsx.Head().Value().Equals(a))
	b := types.NewString("b")
	dsx, ok := dsx.Commit(b)
	assert.True(ok)
	assert.True(dsx.Head().Value().Equals(b))

	// dsy: |a| -> |c|
	assert.True(dsy.Head().Value().Equals(a))
	c := types.NewString("c")
	dsy, ok = dsy.Commit(c)
	assert.False(ok)
	assert.True(dsy.Head().Value().Equals(b))
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, ok = dsy.Commit(c)
	assert.True(ok)
	assert.True(dsy.Head().Value().Equals(c))
}
