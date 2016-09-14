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
	db     Database
	makeDb func(chunks.ChunkStore) Database
}

type LocalDatabaseSuite struct {
	DatabaseSuite
}

func (suite *LocalDatabaseSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDb = NewDatabase
	suite.db = suite.makeDb(suite.cs)
}

type RemoteDatabaseSuite struct {
	DatabaseSuite
}

func (suite *RemoteDatabaseSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDb = func(cs chunks.ChunkStore) Database {
		hbs := newHTTPBatchStoreForTest(cs)
		return &RemoteDatabaseClient{newDatabaseCommon(newCachingChunkHaver(hbs), types.NewValueStore(hbs), hbs)}
	}
	suite.db = suite.makeDb(suite.cs)
}

func (suite *DatabaseSuite) TearDownTest() {
	suite.db.Close()
	suite.cs.Close()
}

func (suite *DatabaseSuite) TestReadWriteCache() {
	var v types.Value = types.Bool(true)
	suite.NotEqual(hash.Hash{}, suite.db.WriteValue(v))
	r := suite.db.WriteValue(v).TargetHash()
	commit := NewCommit(v, types.NewSet(), types.EmptyStruct)
	newDs, err := suite.db.Commit("foo", commit)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	v = newDs.ReadValue(r)
	suite.True(v.Equals(types.Bool(true)))
}

func (suite *DatabaseSuite) TestReadWriteCachePersists() {
	var err error
	var v types.Value = types.Bool(true)
	suite.NotEqual(hash.Hash{}, suite.db.WriteValue(v))
	r := suite.db.WriteValue(v)
	commit := NewCommit(v, types.NewSet(), types.EmptyStruct)
	suite.db, err = suite.db.Commit("foo", commit)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	newCommit := NewCommit(r, types.NewSet(types.NewRef(commit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit("foo", newCommit)
	suite.NoError(err)
}

func (suite *DatabaseSuite) TestWriteRefToNonexistentValue() {
	suite.Panics(func() { suite.db.WriteValue(types.NewRef(types.Bool(true))) })
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.db.ReadValue(hash.Hash{}))
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "ds1"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet(), types.EmptyStruct)
	ds2, err := suite.db.Commit(datasetID, aCommit)
	suite.NoError(err)

	// The old database still has no head.
	_, ok := suite.db.MaybeHead(datasetID)
	suite.False(ok)
	_, ok = suite.db.MaybeHeadRef(datasetID)
	suite.False(ok)

	// The new database has |a|.
	aCommit1 := ds2.Head(datasetID)
	suite.True(aCommit1.Get(ValueField).Equals(a))
	aRef1 := ds2.HeadRef(datasetID)
	suite.Equal(aCommit1.Hash(), aRef1.TargetHash())
	suite.Equal(uint64(1), aRef1.Height())
	suite.db = ds2

	// |a| <- |b|
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))
	suite.Equal(uint64(2), suite.db.HeadRef(datasetID).Height())

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.String("c")
	cCommit := NewCommit(c, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, cCommit)
	suite.Error(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b| <- |d|
	d := types.String("d")
	dCommit := NewCommit(d, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, dCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(d))
	suite.Equal(uint64(3), suite.db.HeadRef(datasetID).Height())

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.Error(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.db.Commit("otherDs", aCommit)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDs := suite.makeDb(suite.cs)
	datasets2 := newDs.Datasets()
	suite.Equal(uint64(2), datasets2.Len())
	newDs.Close()
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	var err error
	a := types.String("a")
	suite.db, err = suite.db.Commit(datasetID1, NewCommit(a, types.NewSet(), types.EmptyStruct))
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID1).Get(ValueField).Equals(a))

	// ds1; |a|, ds2: |b|
	b := types.String("b")
	suite.db, err = suite.db.Commit(datasetID2, NewCommit(b, types.NewSet(), types.EmptyStruct))
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID2).Get(ValueField).Equals(b))

	suite.db, err = suite.db.Delete(datasetID1)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID2).Get(ValueField).Equals(b))
	_, present := suite.db.MaybeHead(datasetID1)
	suite.False(present, "Dataset %s should not be present", datasetID1)

	// Get a fresh database, and verify that only ds1 is present
	newDs := suite.makeDb(suite.cs)
	datasets = newDs.Datasets()
	suite.Equal(uint64(1), datasets.Len())
	_, present = suite.db.MaybeHead(datasetID2)
	suite.True(present, "Dataset %s should be present", datasetID2)
	newDs.Close()
}

func (suite *DatabaseSuite) TestDeleteConcurrentDatabaseUse() {
	datasetID := "ds1"
	suite.Zero(suite.db.Datasets().Len())
	var err error

	// |a|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet(), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	suite.NoError(err)

	// |a| <- |b|
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	db2, err := suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(a))
	suite.True(db2.Head(datasetID).Get(ValueField).Equals(b))

	suite.db, err = suite.db.Delete(datasetID)
	suite.NoError(err)
	_, present := suite.db.MaybeHead(datasetID)
	suite.False(present, "Dataset %s should not be present", datasetID)
	_, present = db2.MaybeHead(datasetID)
	suite.True(present, "Dataset %s should be present", datasetID)

	// Get a fresh database, and verify that no databases are present
	newDb := suite.makeDb(suite.cs)
	suite.Equal(uint64(0), newDb.Datasets().Len())
	newDb.Close()
}

func (suite *DatabaseSuite) TestConcurrentDatabaseUse() {
	datasetID := "ds1"
	var err error

	// Setup:
	// |a| <- |b|
	a := types.String("a")
	aCommit := NewCommit(a, types.NewSet(), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// Important to create this here.
	db2 := suite.makeDb(suite.cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.String("c")
	cCommit := NewCommit(c, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, cCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, Database returned by Commit() should have |c| as Head.
	e := types.String("e")
	eCommit := NewCommit(e, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
	db2, err = db2.Commit(datasetID, eCommit)
	suite.Error(err)
	suite.True(db2.Head(datasetID).Get(ValueField).Equals(c))
}

type waitDuringUpdateRootChunkStore struct {
	chunks.ChunkStore
	preUpdateRootHook func()
}

func (w *waitDuringUpdateRootChunkStore) UpdateRoot(current, last hash.Hash) (ok bool) {
	if w.preUpdateRootHook != nil {
		w.preUpdateRootHook()
	}
	ok = w.ChunkStore.UpdateRoot(current, last)
	return
}

func (suite *DatabaseSuite) TestCommitWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	var err error

	// Setup:
	// ds1: |a| <- |b|
	aCommit := NewCommit(types.String("a"), types.NewSet(), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	w := &waitDuringUpdateRootChunkStore{suite.cs, nil}
	db := suite.makeDb(w)

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b|
	// ds2: |stuff|
	w.preUpdateRootHook = func() {
		var concErr error
		e := types.String("stuff")
		eCommit := NewCommit(e, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
		suite.db, concErr = suite.db.Commit("ds2", eCommit)
		suite.NoError(concErr)
		suite.True(suite.db.Head("ds2").Get(ValueField).Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should proceed without a problem
	c := types.String("c")
	cCommit := NewCommit(c, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
	db, err = db.Commit(datasetID, cCommit)
	suite.NoError(err)
	suite.True(db.Head(datasetID).Get(ValueField).Equals(c))

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |c| <- |e|
	e := types.String("e")
	w.preUpdateRootHook = func() {
		var concErr error
		eCommit := NewCommit(e, types.NewSet(types.NewRef(cCommit)), types.EmptyStruct)
		suite.db, concErr = suite.db.Commit(datasetID, eCommit)
		suite.NoError(concErr)
		suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should fail due to the above
	nope := types.String("nope")
	nopeCommit := NewCommit(nope, types.NewSet(types.NewRef(cCommit)), types.EmptyStruct)
	db, err = db.Commit(datasetID, nopeCommit)
	suite.Error(err)
	suite.True(db.Head(datasetID).Get(ValueField).Equals(e))
}

func (suite *DatabaseSuite) TestDeleteWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	var err error

	// Setup:
	// ds1: |a| <- |b|
	aCommit := NewCommit(types.String("a"), types.NewSet(), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	b := types.String("b")
	bCommit := NewCommit(b, types.NewSet(types.NewRef(aCommit)), types.EmptyStruct)
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	w := &waitDuringUpdateRootChunkStore{suite.cs, nil}
	db := suite.makeDb(w)

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	w.preUpdateRootHook = func() {
		var concErr error
		eCommit := NewCommit(e, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
		suite.db, concErr = suite.db.Commit(datasetID, eCommit)
		suite.NoError(concErr)
		suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should fail due to the above
	db, err = db.Delete(datasetID)
	suite.Error(err)
	suite.True(db.Head(datasetID).Get(ValueField).Equals(e))

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b| <- |e|
	// ds2: |stuff|
	w.preUpdateRootHook = func() {
		var concErr error
		e := types.String("stuff")
		eCommit := NewCommit(e, types.NewSet(types.NewRef(bCommit)), types.EmptyStruct)
		suite.db, concErr = suite.db.Commit("ds2", eCommit)
		suite.NoError(concErr)
		suite.True(suite.db.Head("ds2").Get(ValueField).Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should proceed without a problem
	db, err = db.Delete(datasetID)
	suite.NoError(err)
	_, present := db.MaybeHead(datasetID)
	suite.False(present, "Dataset %s should not be present", datasetID)
}

func (suite *DatabaseSuite) TestDatabaseHeightOfRefs() {
	r1 := suite.db.WriteValue(types.String("hello"))
	suite.Equal(uint64(1), r1.Height())

	r2 := suite.db.WriteValue(r1)
	suite.Equal(uint64(2), r2.Height())
	suite.Equal(uint64(3), suite.db.WriteValue(r2).Height())
}

func (suite *DatabaseSuite) TestDatabaseHeightOfCollections() {
	setOfStringType := types.MakeSetType(types.StringType)
	setOfRefOfStringType := types.MakeSetType(types.MakeRefType(types.StringType))

	// Set<String>
	v1 := types.String("hello")
	v2 := types.String("world")
	s1 := types.NewSet(v1, v2)
	suite.Equal(uint64(1), suite.db.WriteValue(s1).Height())

	// Set<Ref<String>>
	s2 := types.NewSet(suite.db.WriteValue(v1), suite.db.WriteValue(v2))
	suite.Equal(uint64(2), suite.db.WriteValue(s2).Height())

	// List<Set<String>>
	v3 := types.String("foo")
	v4 := types.String("bar")
	s3 := types.NewSet(v3, v4)
	l1 := types.NewList(s1, s3)
	suite.Equal(uint64(1), suite.db.WriteValue(l1).Height())

	// List<Ref<Set<String>>
	l2 := types.NewList(suite.db.WriteValue(s1), suite.db.WriteValue(s3))
	suite.Equal(uint64(2), suite.db.WriteValue(l2).Height())

	// List<Ref<Set<Ref<String>>>
	s4 := types.NewSet(suite.db.WriteValue(v3), suite.db.WriteValue(v4))
	l3 := types.NewList(suite.db.WriteValue(s4))
	suite.Equal(uint64(3), suite.db.WriteValue(l3).Height())

	// List<Set<String> | RefValue<Set<String>>>
	l4 := types.NewList(s1, suite.db.WriteValue(s3))
	suite.Equal(uint64(2), suite.db.WriteValue(l4).Height())
	l5 := types.NewList(suite.db.WriteValue(s1), s3)
	suite.Equal(uint64(2), suite.db.WriteValue(l5).Height())

	// Familiar with the "New Jersey Turnpike" drink? Here's the noms version of that...
	everything := []types.Value{v1, v2, s1, s2, v3, v4, s3, l1, l2, s4, l3, l4, l5}
	andMore := make([]types.Value, 0, len(everything)*3+2)
	for _, v := range everything {
		andMore = append(andMore, v, v.Type(), suite.db.WriteValue(v))
	}
	andMore = append(andMore, setOfStringType, setOfRefOfStringType)

	suite.db.WriteValue(types.NewList(andMore...))
}
