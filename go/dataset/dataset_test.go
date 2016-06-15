// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestDatasetCommitTracker(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	cs := chunks.NewMemoryStore()

	ds1 := NewDataset(datas.NewDatabase(cs), id1)
	ds1Commit := types.NewString("Commit value for " + id1)
	ds1, err := ds1.Commit(ds1Commit)
	assert.NoError(err)

	ds2 := NewDataset(datas.NewDatabase(cs), id2)
	ds2Commit := types.NewString("Commit value for " + id2)
	ds2, err = ds2.Commit(ds2Commit)
	assert.NoError(err)

	assert.EqualValues(ds1Commit, ds1.Head().Get(datas.ValueField))
	assert.EqualValues(ds2Commit, ds2.Head().Get(datas.ValueField))
	assert.False(ds2.Head().Get(datas.ValueField).Equals(ds1Commit))
	assert.False(ds1.Head().Get(datas.ValueField).Equals(ds2Commit))

	assert.Equal("sha1-898dfd332626292e92cd4a5d85e5c486dce1d57f", cs.Root().String())
}

func newDS(id string, cs *chunks.MemoryStore) Dataset {
	store := datas.NewDatabase(cs)
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
	assert.True(ds1.Head().Get(datas.ValueField).Equals(a))

	// ds1: |a|
	//        \ds2
	ds2 := newDS(id2, cs)
	ds2, err = ds2.Commit(ds1.Head().Get(datas.ValueField))
	assert.NoError(err)
	assert.True(ds2.Head().Get(datas.ValueField).Equals(a))

	// ds1: |a| <- |b|
	b := types.NewString("b")
	ds1, err = ds1.Commit(b)
	assert.NoError(err)
	assert.True(ds1.Head().Get(datas.ValueField).Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.NewString("c")
	ds2, err = ds2.Commit(c)
	assert.NoError(err)
	assert.True(ds2.Head().Get(datas.ValueField).Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := types.NewSet(types.NewRef(ds1.Head()), types.NewRef(ds2.Head()))
	d := types.NewString("d")
	ds2, err = ds2.CommitWithParents(d, mergeParents)
	assert.NoError(err)
	assert.True(ds2.Head().Get(datas.ValueField).Equals(d))

	ds1, err = ds1.CommitWithParents(d, mergeParents)
	assert.NoError(err)
	assert.True(ds1.Head().Get(datas.ValueField).Equals(d))
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
	assert.True(dsx.Head().Get(datas.ValueField).Equals(a))

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
	assert.True(dsy.Head().Get(datas.ValueField).Equals(b))
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
		assert.True(ds1.Head().Get(datas.ValueField).Equals(a))
	}

	dsx := newDS(id1, cs)
	dsy := newDS(id1, cs)

	// dsx: |a| -> |b|
	assert.True(dsx.Head().Get(datas.ValueField).Equals(a))
	b := types.NewString("b")
	dsx, err := dsx.Commit(b)
	assert.NoError(err)
	assert.True(dsx.Head().Get(datas.ValueField).Equals(b))

	// dsy: |a| -> |c|
	assert.True(dsy.Head().Get(datas.ValueField).Equals(a))
	c := types.NewString("c")
	dsy, err = dsy.Commit(c)
	assert.Error(err)
	assert.True(dsy.Head().Get(datas.ValueField).Equals(b))
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, err = dsy.Commit(c)
	assert.NoError(err)
	assert.True(dsy.Head().Get(datas.ValueField).Equals(c))
}

func TestIdValidation(t *testing.T) {
	assert := assert.New(t)
	store := datas.NewDatabase(chunks.NewMemoryStore())

	invalidDatasetNames := []string{" ", "", "a ", " a", "$", "#", ":", "\n", "💩"}
	for _, id := range invalidDatasetNames {
		assert.Panics(func() {
			NewDataset(store, id)
		})
	}
}

func TestHeadValueFunctions(t *testing.T) {
	assert := assert.New(t)

	id1 := "testdataset"
	id2 := "otherdataset"
	cs := chunks.NewMemoryStore()

	ds1 := newDS(id1, cs)

	// ds1: |a|
	a := types.NewString("a")
	ds1, err := ds1.Commit(a)
	assert.NoError(err)

	hv := ds1.Head().Get(datas.ValueField)
	assert.Equal(a, hv)
	assert.Equal(a, ds1.HeadValue())

	hv, ok := ds1.MaybeHeadValue()
	assert.True(ok)
	assert.Equal(a, hv)

	ds2 := newDS(id2, cs)
	assert.Panics(func() {
		ds2.HeadValue()
	})
	_, ok = ds2.MaybeHeadValue()
	assert.False(ok)
}
