// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func mustGetValue(v types.Value, found bool, err error) types.Value {
	d.PanicIfError(err)
	d.PanicIfFalse(found)
	return v
}

func mustGetCommittedValue(db *database, v types.Value) types.Value {
	r, err := GetCommittedValue(context.Background(), db, v)
	d.PanicIfError(err)
	d.PanicIfFalse(r != nil)
	return r
}

func mustHeadAddr(ds Dataset) hash.Hash {
	h, ok := ds.MaybeHeadAddr()
	d.PanicIfFalse(ok)
	return h
}

func TestExplicitBranchUsingDatasets(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	id2 := "othertestdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewViewWithDefaultFormat()).(*database)
	defer store.Close()

	ds1, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// ds1: |a|
	a := types.String("a")
	ds1, err = CommitValue(context.Background(), store, ds1, a)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(ds1.db, mustHead(ds1)).Equals(a))

	// ds1: |a|
	//        \ds2
	ds2, err := store.GetDataset(context.Background(), id2)
	assert.NoError(err)
	ds2, err = store.Commit(context.Background(), ds2, mustHeadValue(ds1), CommitOptions{Parents: []hash.Hash{mustHeadAddr(ds1)}})
	assert.NoError(err)
	assert.True(mustGetCommittedValue(store, mustHead(ds2)).Equals(a))

	// ds1: |a| <- |b|
	b := types.String("b")
	ds1, err = CommitValue(context.Background(), store, ds1, b)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(store, mustHead(ds1)).Equals(b))

	// ds1: |a|    <- |b|
	//        \ds2 <- |c|
	c := types.String("c")
	ds2, err = CommitValue(context.Background(), store, ds2, c)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(store, mustHead(ds2)).Equals(c))

	// ds1: |a|    <- |b| <--|d|
	//        \ds2 <- |c| <--/
	d := types.String("d")
	ds2, err = store.Commit(context.Background(), ds2, d, CommitOptions{Parents: []hash.Hash{mustHeadAddr(ds1), mustHeadAddr(ds2)}})
	assert.NoError(err)
	assert.True(mustGetCommittedValue(store, mustHead(ds2)).Equals(d))

	ds1, err = store.Commit(context.Background(), ds1, d, CommitOptions{Parents: []hash.Hash{mustHeadAddr(ds1), mustHeadAddr(ds2)}})
	assert.NoError(err)
	assert.True(mustGetCommittedValue(store, mustHead(ds1)).Equals(d))
}

func TestTwoClientsWithEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewViewWithDefaultFormat())
	defer store.Close()

	dsx, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// dsx: || -> |a|
	a := types.String("a")
	dsx, err = CommitValue(context.Background(), store, dsx, a)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(dsx.db, mustHead(dsx)).Equals(a))

	// dsy: || -> |b|
	_, ok := dsy.MaybeHead()
	assert.False(ok)
	b := types.String("b")
	_, err = CommitValue(context.Background(), store, dsy, b)
	assert.Error(err)

	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |a| -> |b|
	dsy, err = store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err = CommitValue(context.Background(), store, dsy, b)
	assert.NoError(err)
	headVal := mustHeadValue(dsy)
	assert.True(headVal.Equals(b))
}

func TestTwoClientsWithNonEmptyDataset(t *testing.T) {
	assert := assert.New(t)
	id1 := "testdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewViewWithDefaultFormat())
	defer store.Close()

	a := types.String("a")
	{
		// ds1: || -> |a|
		ds1, err := store.GetDataset(context.Background(), id1)
		assert.NoError(err)
		ds1, err = CommitValue(context.Background(), store, ds1, a)
		assert.NoError(err)
		assert.True(mustGetCommittedValue(ds1.db, mustHead(ds1)).Equals(a))
	}

	dsx, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)

	// dsx: |a| -> |b|
	assert.True(mustGetCommittedValue(dsx.db, mustHead(dsx)).Equals(a))
	b := types.String("b")
	dsx, err = CommitValue(context.Background(), store, dsx, b)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(dsx.db, mustHead(dsx)).Equals(b))

	// dsy: |a| -> |c|
	assert.True(mustGetCommittedValue(dsy.db, mustHead(dsy)).Equals(a))
	c := types.String("c")
	_, err = CommitValue(context.Background(), store, dsy, c)
	assert.Error(err)
	// Commit failed, but dsy now has latest head, so we should be able to just try again.
	// dsy: |b| -> |c|
	dsy, err = store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	dsy, err = CommitValue(context.Background(), store, dsy, c)
	assert.NoError(err)
	assert.True(mustGetCommittedValue(dsy.db, mustHead(dsy)).Equals(c))
}

func TestIdValidation(t *testing.T) {
	assert := assert.New(t)
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewViewWithDefaultFormat())

	invalidDatasetNames := []string{" ", "", "a ", " a", "$", "#", ":", "\n", "💩"}
	for _, id := range invalidDatasetNames {
		_, err := store.GetDataset(context.Background(), id)
		assert.Error(err)
	}
}

func TestHeadValueFunctions(t *testing.T) {
	assert := assert.New(t)

	id1 := "testdataset"
	id2 := "otherdataset"
	stg := &chunks.MemoryStorage{}
	store := NewDatabase(stg.NewViewWithDefaultFormat())
	defer store.Close()

	ds1, err := store.GetDataset(context.Background(), id1)
	assert.NoError(err)
	assert.False(ds1.HasHead())

	// ds1: |a|
	a := types.String("a")
	ds1, err = CommitValue(context.Background(), store, ds1, a)
	assert.NoError(err)
	assert.True(ds1.HasHead())

	hv, err := GetCommittedValue(context.Background(), ds1.db, mustHead(ds1))
	assert.NoError(err)
	assert.Equal(a, hv)
	assert.Equal(a, mustHeadValue(ds1))

	hv, ok, err := ds1.MaybeHeadValue()
	assert.NoError(err)
	assert.True(ok)
	assert.Equal(a, hv)

	ds2, err := store.GetDataset(context.Background(), id2)
	assert.NoError(err)
	_, ok, err = ds2.MaybeHeadValue()
	assert.NoError(err)
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
