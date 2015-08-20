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
	ms := &chunks.MemoryStore{}

	ds1 := NewDataset(datas.NewDataStore(ms), id1)
	ds1Commit := types.NewString("Commit value for " + id1)
	ds1, ok := ds1.Commit(
		datas.NewCommit().SetParents(ds1.HeadAsSet()).SetValue(ds1Commit))
	assert.True(ok)

	ds2 := NewDataset(datas.NewDataStore(ms), id2)
	ds2Commit := types.NewString("Commit value for " + id2)
	ds2, ok = ds2.Commit(
		datas.NewCommit().SetParents(ds2.HeadAsSet()).SetValue(ds2Commit))
	assert.True(ok)

	assert.EqualValues(ds1Commit, ds1.Head().Value())
	assert.EqualValues(ds2Commit, ds2.Head().Value())
	assert.False(ds2.Head().Value().Equals(ds1Commit))
	assert.False(ds1.Head().Value().Equals(ds2Commit))

	assert.Equal("sha1-8aba7db6a2e7769afdb0b6ba3eabfe9b4624d83f", ms.Root().String())
}

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	ms := &chunks.MemoryStore{}

	getDS := func(id string) Dataset {
		store := datas.NewDataStore(ms)
		return NewDataset(store, id)
	}
	ds1 := getDS(id1)

	// ds1: |a|
	a := datas.NewCommit().SetParents(ds1.HeadAsSet()).SetValue(types.NewString("a"))
	ds1, ok := ds1.Commit(a)
	assert.True(ok)
	assert.True(ds1.Head().Equals(a))

	// ds1: |a|
	//        \ds2
	ds2 := getDS(id2)
	ds2, ok = ds2.Commit(ds1.Head())
	assert.True(ok)
	assert.True(ds2.Head().Equals(a))

	// ds1: |a| <- |b|
	b := datas.NewCommit().SetParents(ds1.HeadAsSet()).SetValue(types.NewString("b"))
	ds1, ok = ds1.Commit(b)
	assert.False(ok)
	assert.True(ds1.Head().Equals(a))
	// Commit failed, but ds1 didn't change, so we should be able to just try again.
	ds1, ok = ds1.Commit(b)
	assert.True(ok)
	assert.True(ds1.Head().Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := datas.NewCommit().SetParents(ds2.HeadAsSet()).SetValue(types.NewString("c"))
	ds2, ok = ds2.Commit(c)
	assert.False(ok)
	assert.True(ds2.Head().Equals(a))
	// Commit failed, but ds2 didn't change, so we should be able to just try again.
	ds2, ok = ds2.Commit(c)
	assert.True(ok)
	assert.True(ds2.Head().Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := datas.NewSetOfCommit().Insert(ds1.Head()).Insert(ds2.Head()).NomsValue()
	d := datas.NewCommit().SetParents(mergeParents).SetValue(types.NewString("d"))
	ds2, ok = ds2.Commit(d)
	assert.True(ok)
	assert.True(ds2.Head().Equals(d))

	ds1, ok = ds1.Commit(d)
	assert.False(ok)
	assert.True(ds1.Head().Equals(b))
	// Commit failed, but while ds2 changed, ds1 didn't.
	ds1, ok = ds1.Commit(d)
	assert.True(ok)
	assert.True(ds1.Head().Equals(d))

}
