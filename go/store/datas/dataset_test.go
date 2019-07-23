// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewView())
	defer store.Close()

	ds1, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// ds1: |a|
	a := types.String("a")
	ds1, err = store.CommitValue(context.Background(), ds1, a)
	assert.NoError(err)
	assert.True(mustHead(ds1).Get(ValueField).Equals(a))

	// ds1: |a|
	//        \ds2
	ds2, err := store.GetDataset(context.Background(), id2)
	assert.NoError(err)
	ds2, err = store.Commit(context.Background(), ds2, mustHeadValue(ds1), CommitOptions{Parents: types.NewSet(context.Background(), store, mustHeadRef(ds1))})
	assert.NoError(err)
	assert.True(mustHead(ds2).Get(ValueField).Equals(a))

	// ds1: |a| <- |b|
	b := types.String("b")
	ds1, err = store.CommitValue(context.Background(), ds1, b)
	assert.NoError(err)
	assert.True(mustHead(ds1).Get(ValueField).Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.String("c")
	ds2, err = store.CommitValue(context.Background(), ds2, c)
	assert.NoError(err)
	assert.True(mustHead(ds2).Get(ValueField).Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	mergeParents := types.NewSet(context.Background(), store, types.NewRef(mustHead(ds1), types.Format_7_18), types.NewRef(mustHead(ds2), types.Format_7_18))
	d := types.String("d")
	ds2, err = store.Commit(context.Background(), ds2, d, CommitOptions{Parents: mergeParents})
	assert.NoError(err)
	assert.True(mustHead(ds2).Get(ValueField).Equals(d))

	ds1, err = store.Commit(context.Background(), ds1, d, CommitOptions{Parents: mergeParents})
	assert.NoError(err)
	assert.True(mustHead(ds1).Get(ValueField).Equals(d))
}

func TestTwoClientsWithEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewView())
	defer store.Close()

	dsx, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// dsx: || -> |a|
	a := types.String("a")
	dsx, err = store.CommitValue(context.Background(), dsx, a)
	assert.NoError(err)
	assert.True(mustHead(dsx).Get(ValueField).Equals(a))

	// dsy: || -> |b|
	_, ok := dsy.MaybeHead()
	assert.False(ok)
	b := types.String("b")
	_, err = store.CommitValue(context.Background(), dsy, b)
	assert.Error(err)

	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |a| -> |b|
	dsy, err = store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err = store.CommitValue(context.Background(), dsy, b)
	assert.NoError(err)
	headVal := mustHeadValue(dsy)
	assert.True(headVal.Equals(b))
}

func TestTwoClientsWithNonEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewView())
	defer store.Close()

	a := types.String("a")
	{
		// ds1: || -> |a|
		ds1, err := store.GetDataset(context.Background(), id1)
		assert.NoError(err)
		ds1, err = store.CommitValue(context.Background(), ds1, a)
		assert.NoError(err)
		assert.True(mustHead(ds1).Get(ValueField).Equals(a))
	}

	dsx, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// dsx: |a| -> |b|
	assert.True(mustHead(dsx).Get(ValueField).Equals(a))
	b := types.String("b")
	dsx, err = store.CommitValue(context.Background(), dsx, b)
	assert.NoError(err)
	assert.True(mustHead(dsx).Get(ValueField).Equals(b))

	// dsy: |a| -> |c|
	assert.True(mustHead(dsy).Get(ValueField).Equals(a))
	c := types.String("c")
	_, err = store.CommitValue(context.Background(), dsy, c)
	assert.Error(err)
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, err = store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err = store.CommitValue(context.Background(), dsy, c)
	assert.NoError(err)
	assert.True(mustHead(dsy).Get(ValueField).Equals(c))
}

func TestIdValidation(t *testing.T) {
	assert := assert.New(t)
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewView())

	invalidDatasetNames := []string{" ", "", "a ", " a", "$", "#", ":", "\n", "💩"}
	for _, id := range invalidDatasetNames {
		assert.Panics(func() {
			store.GetDataset(context.Background(), id)
		})
	}
}

func TestHeadValueFunctions(t *testing.T) {
	assert := assert.New(t)

	id1 := "testdataset"
	id2 := "otherdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewView())
	defer store.Close()

	ds1, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	assert.False(ds1.HasHead())

	// ds1: |a|
	a := types.String("a")
	ds1, err = store.CommitValue(context.Background(), ds1, a)
	assert.NoError(err)
	assert.True(ds1.HasHead())

	hv := mustHead(ds1).Get(ValueField)
	assert.Equal(a, hv)
	assert.Equal(a, mustHeadValue(ds1))

	hv, ok := ds1.MaybeHeadValue()
	assert.True(ok)
	assert.Equal(a, hv)

	ds2, err := store.GetDataset(context.Background(), id2)
	assert.NoError(err)
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
