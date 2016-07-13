// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
)

// writesOnCommit allows tests to adjust for how many writes databaseCommon performs on Commit()
const writesOnCommit = 2

func TestLocalDatabase(t *testing.T) {
	suite.Run(t, &LocalDatabaseSuite{})
}

func TestRemoteDatabase(t *testing.T) {
	suite.Run(t, &RemoteDatabaseSuite{})
}

type DatabaseSuite struct {
	suite.Suite
	cs     *chunks.TestStore
	ds     Database
	makeDs func(chunks.ChunkStore) Database
}

type LocalDatabaseSuite struct {
	DatabaseSuite
}

func (suite *LocalDatabaseSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDs = NewDatabase
	suite.ds = suite.makeDs(suite.cs)
}

type RemoteDatabaseSuite struct {
	DatabaseSuite
}

func (suite *RemoteDatabaseSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDs = func(cs chunks.ChunkStore) Database {
		hbs := newHTTPBatchStoreForTest(cs)
		return &RemoteDatabaseClient{newDatabaseCommon(newCachingChunkHaver(hbs), types.NewValueStore(hbs), hbs)}
	}
	suite.ds = suite.makeDs(suite.cs)
}

func (suite *DatabaseSuite) TearDownTest() {
	suite.ds.Close()
	suite.cs.Close()
}

func (suite *DatabaseSuite) TestReadWriteCache() {
	var v types.Value = types.Bool(true)
	suite.NotEqual(hash.Hash{}, suite.ds.WriteValue(v))
	r := suite.ds.WriteValue(v).TargetHash()
	commit := NewCommit(v, types.NewSet())
	newDs, err := suite.ds.Commit("foo", commit)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	v = newDs.ReadValue(r)
	suite.True(v.Equals(types.Bool(true)))
}

func (suite *DatabaseSuite) TestReadWriteCachePersists() {
	var err error
	var v types.Value = types.Bool(true)
	suite.NotEqual(hash.Hash{}, suite.ds.WriteValue(v))
	r := suite.ds.WriteValue(v)
	commit := NewCommit(v, types.NewSet())
	suite.ds, err = suite.ds.Commit("foo", commit)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	newCommit := NewCommit(r, types.NewSet(types.NewRef(commit)))
	suite.ds, err = suite.ds.Commit("foo", newCommit)
	suite.NoError(err)
}

func (suite *DatabaseSuite) TestWriteRefToNonexistentValue() {
	suite.Panics(func() { suite.ds.WriteValue(types.NewRef(types.Bool(true))) })
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.ds.ReadValue(hash.Hash{}))
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "ds1"
	datasets := suite.ds.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet())
	ds2, err := suite.ds.Commit(datasetID, aCommit)
	suite.NoError(err)

	// The old database still has no head.
	_, ok := suite.ds.MaybeHead(datasetID)
	suite.False(ok)
	_, ok = suite.ds.MaybeHeadRef(datasetID)
	suite.False(ok)

	// The new database has |a|.
	aCommit1 := ds2.Head(datasetID)
	suite.True(aCommit1.Get(ValueField).Equals(a))
	aRef1 := ds2.HeadRef(datasetID)
	suite.Equal(aCommit1.Hash(), aRef1.TargetHash())
	suite.Equal(uint64(1), aRef1.Height())
	suite.ds = ds2

	// |a| <- |b|
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))
	suite.Equal(uint64(2), suite.ds.HeadRef(datasetID).Height())

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.String("c")
	cCommit := NewCommit(c, types.NewSet(types.NewRef(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, cCommit)
	suite.Error(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b| <- |d|
	d := types.String("d")
	dCommit := NewCommit(d, types.NewSet(types.NewRef(bCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, dCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(d))
	suite.Equal(uint64(3), suite.ds.HeadRef(datasetID).Height())

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.Error(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.ds.Commit("otherDs", aCommit)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDs := suite.makeDs(suite.cs)
	datasets2 := newDs.Datasets()
	suite.Equal(uint64(2), datasets2.Len())
	newDs.Close()
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	datasets := suite.ds.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	var err error
	a := types.String("a")
	suite.ds, err = suite.ds.Commit(datasetID1, NewCommit(a, types.NewSet()))
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID1).Get(ValueField).Equals(a))

	// ds1; |a|, ds2: |b|
	b := types.String("b")
	suite.ds, err = suite.ds.Commit(datasetID2, NewCommit(b, types.NewSet()))
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID2).Get(ValueField).Equals(b))

	suite.ds, err = suite.ds.Delete(datasetID1)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID2).Get(ValueField).Equals(b))
	_, present := suite.ds.MaybeHead(datasetID1)
	suite.False(present, "Dataset %s should not be present", datasetID1)

	// Get a fresh database, and verify that only ds1 is present
	newDs := suite.makeDs(suite.cs)
	datasets = newDs.Datasets()
	suite.Equal(uint64(1), datasets.Len())
	_, present = suite.ds.MaybeHead(datasetID2)
	suite.True(present, "Dataset %s should be present", datasetID2)
	newDs.Close()
}

func (suite *DatabaseSuite) TestDatabaseDeleteConcurrent() {
	datasetID := "ds1"
	suite.Zero(suite.ds.Datasets().Len())
	var err error

	// |a|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet())
	suite.ds, err = suite.ds.Commit(datasetID, aCommit)
	suite.NoError(err)

	// |a| <- |b|
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)))
	ds2, err := suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(a))
	suite.True(ds2.Head(datasetID).Get(ValueField).Equals(b))

	suite.ds, err = suite.ds.Delete(datasetID)
	suite.NoError(err)
	_, present := suite.ds.MaybeHead(datasetID)
	suite.False(present, "Dataset %s should not be present", datasetID)
	_, present = ds2.MaybeHead(datasetID)
	suite.True(present, "Dataset %s should be present", datasetID)

	// Get a fresh database, and verify that no databases are present
	newDs := suite.makeDs(suite.cs)
	suite.Equal(uint64(0), newDs.Datasets().Len())
	newDs.Close()
}

func (suite *DatabaseSuite) TestDatabaseConcurrency() {
	datasetID := "ds1"
	var err error

	// Setup:
	// |a| <- |b|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet())
	suite.ds, err = suite.ds.Commit(datasetID, aCommit)
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))

	// Important to create this here.
	ds2 := suite.makeDs(suite.cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.String("c")
	cCommit := NewCommit(c, types.NewSet(types.NewRef(bCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, cCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, Database returned by Commit() should have |c| as Head.
	e := types.String("e")
	eCommit := NewCommit(e, types.NewSet(types.NewRef(bCommit)))
	ds2, err = ds2.Commit(datasetID, eCommit)
	suite.Error(err)
	suite.True(ds2.Head(datasetID).Get(ValueField).Equals(c))
}

func (suite *DatabaseSuite) TestDatabaseHeightOfRefs() {
	r1 := suite.ds.WriteValue(types.String("hello"))
	suite.Equal(uint64(1), r1.Height())

	r2 := suite.ds.WriteValue(r1)
	suite.Equal(uint64(2), r2.Height())
	suite.Equal(uint64(3), suite.ds.WriteValue(r2).Height())
}

func (suite *DatabaseSuite) TestDatabaseHeightOfCollections() {
	setOfStringType := types.MakeSetType(types.StringType)
	setOfRefOfStringType := types.MakeSetType(types.MakeRefType(types.StringType))

	// Set<String>
	v1 := types.String("hello")
	v2 := types.String("world")
	s1 := types.NewSet(v1, v2)
	suite.Equal(uint64(1), suite.ds.WriteValue(s1).Height())

	// Set<Ref<String>>
	s2 := types.NewSet(suite.ds.WriteValue(v1), suite.ds.WriteValue(v2))
	suite.Equal(uint64(2), suite.ds.WriteValue(s2).Height())

	// List<Set<String>>
	v3 := types.String("foo")
	v4 := types.String("bar")
	s3 := types.NewSet(v3, v4)
	l1 := types.NewList(s1, s3)
	suite.Equal(uint64(1), suite.ds.WriteValue(l1).Height())

	// List<Ref<Set<String>>
	l2 := types.NewList(types.MakeListType(types.MakeRefType(setOfStringType)),
		suite.ds.WriteValue(s1), suite.ds.WriteValue(s3))
	suite.Equal(uint64(2), suite.ds.WriteValue(l2).Height())

	// List<Ref<Set<Ref<String>>>
	s4 := types.NewSet(suite.ds.WriteValue(v3), suite.ds.WriteValue(v4))
	l3 := types.NewList(types.MakeListType(types.MakeRefType(setOfRefOfStringType)),
		suite.ds.WriteValue(s4))
	suite.Equal(uint64(3), suite.ds.WriteValue(l3).Height())

	// List<Set<String> | RefValue<Set<String>>>.
	l4 := types.NewList(s1, suite.ds.WriteValue(s3))
	suite.Equal(uint64(2), suite.ds.WriteValue(l4).Height())
	l5 := types.NewList(suite.ds.WriteValue(s1), s3)
	suite.Equal(uint64(2), suite.ds.WriteValue(l5).Height())
}
