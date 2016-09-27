// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	store := NewDatabase(chunks.NewMemoryStore())
	defer store.Close()

	ds1 := store.GetDataset(id1)

	// ds1: |a|
	a := types.String("a")
	ds1, err := store.CommitValue(ds1, a)
	assert.NoError(err)
	assert.True(ds1.Head().Get(ValueField).Equals(a))

	// ds1: |a|
	//        \ds2
	ds2 := store.GetDataset(id2)
	ds2, err = store.Commit(ds2, ds1.HeadValue(), CommitOptions{Parents: types.NewSet(ds1.HeadRef())})
	assert.NoError(err)
	assert.True(ds2.Head().Get(ValueField).Equals(a))

	// ds1: |a| <- |b|
	b := types.String("b")
	ds1, err = store.CommitValue(ds1, b)
	assert.NoError(err)
	assert.True(ds1.Head().Get(ValueField).Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.String("c")
	ds2, err = store.CommitValue(ds2, c)
	assert.NoError(err)
	assert.True(ds2.Head().Get(ValueField).Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := types.NewSet(types.NewRef(ds1.Head()), types.NewRef(ds2.Head()))
	d := types.String("d")
	ds2, err = store.Commit(ds2, d, CommitOptions{Parents: mergeParents})
	assert.NoError(err)
	assert.True(ds2.Head().Get(ValueField).Equals(d))

	ds1, err = store.Commit(ds1, d, CommitOptions{Parents: mergeParents})
	assert.NoError(err)
	assert.True(ds1.Head().Get(ValueField).Equals(d))
}

func TestTwoClientsWithEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	store := NewDatabase(chunks.NewMemoryStore())
	defer store.Close()

	dsx := store.GetDataset(id1)
	dsy := store.GetDataset(id1)

	// dsx: || -> |a|
	a := types.String("a")
	dsx, err := store.CommitValue(dsx, a)
	assert.NoError(err)
	assert.True(dsx.Head().Get(ValueField).Equals(a))

	// dsy: || -> |b|
	_, ok := dsy.MaybeHead()
	assert.False(ok)
	b := types.String("b")
	dsy, err = store.CommitValue(dsy, b)
	assert.Error(err)
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |a| -> |b|
	dsy, err = store.CommitValue(dsy, b)
	assert.NoError(err)
	assert.True(dsy.Head().Get(ValueField).Equals(b))
}

func TestTwoClientsWithNonEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	store := NewDatabase(chunks.NewMemoryStore())
	defer store.Close()

	a := types.String("a")
	{
		// ds1: || -> |a|
		ds1 := store.GetDataset(id1)
		ds1, err := store.CommitValue(ds1, a)
		assert.NoError(err)
		assert.True(ds1.Head().Get(ValueField).Equals(a))
	}

	dsx := store.GetDataset(id1)
	dsy := store.GetDataset(id1)

	// dsx: |a| -> |b|
	assert.True(dsx.Head().Get(ValueField).Equals(a))
	b := types.String("b")
	dsx, err := store.CommitValue(dsx, b)
	assert.NoError(err)
	assert.True(dsx.Head().Get(ValueField).Equals(b))

	// dsy: |a| -> |c|
	assert.True(dsy.Head().Get(ValueField).Equals(a))
	c := types.String("c")
	dsy, err = store.CommitValue(dsy, c)
	assert.Error(err)
	assert.True(dsy.Head().Get(ValueField).Equals(b))
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, err = store.CommitValue(dsy, c)
	assert.NoError(err)
	assert.True(dsy.Head().Get(ValueField).Equals(c))
}

func TestIdValidation(t *testing.T) {
	assert := assert.New(t)
	store := NewDatabase(chunks.NewMemoryStore())

	invalidDatasetNames := []string{" ", "", "a ", " a", "$", "#", ":", "\n", "💩"}
	for _, id := range invalidDatasetNames {
		assert.Panics(func() {
			store.GetDataset(id)
		})
	}
}

func TestHeadValueFunctions(t *testing.T) {
	assert := assert.New(t)

	id1 := "testdataset"
	id2 := "otherdataset"
	store := NewDatabase(chunks.NewMemoryStore())
	defer store.Close()

	ds1 := store.GetDataset(id1)

	// ds1: |a|
	a := types.String("a")
	ds1, err := store.CommitValue(ds1, a)
	assert.NoError(err)

	hv := ds1.Head().Get(ValueField)
	assert.Equal(a, hv)
	assert.Equal(a, ds1.HeadValue())

	hv, ok := ds1.MaybeHeadValue()
	assert.True(ok)
	assert.Equal(a, hv)

	ds2 := store.GetDataset(id2)
	assert.Panics(func() {
		ds2.HeadValue()
	})
	_, ok = ds2.MaybeHeadValue()
	assert.False(ok)
}

func TestIsValidDatasetName(t *testing.T) {
	assert := assert.New(t)
	cases := []struct {
		name  string
		valid bool
	}{
		{"foo", true},
		{"foo/bar", true},
		{"f1", true},
		{"1f", true},
		{"", false},
		{"f!!", false},
	}
	for _, c := range cases {
		assert.Equal(c.valid, IsValidDatasetName(c.name),
			"Expected %s validity to be %t", c.name, c.valid)
	}
}
