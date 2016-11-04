// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
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

func TestValidateRef(t *testing.T) {
	db := newLocalDatabase(chunks.NewTestStore())
	defer db.Close()
	b := types.Bool(true)
	r := db.WriteValue(b)

	assert.Panics(t, func() { db.validateRefAsCommit(r) })
	assert.Panics(t, func() { db.validateRefAsCommit(types.NewRef(b)) })
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
	ds := suite.db.GetDataset("foo")
	_, err := suite.db.CommitValue(ds, v)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	v = suite.db.ReadValue(r)
	suite.True(v.Equals(types.Bool(true)))
}

func (suite *DatabaseSuite) TestReadWriteCachePersists() {
	var err error
	var v types.Value = types.Bool(true)
	suite.NotEqual(hash.Hash{}, suite.db.WriteValue(v))
	r := suite.db.WriteValue(v)
	ds := suite.db.GetDataset("foo")
	ds, err = suite.db.CommitValue(ds, v)
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	// Explicitly commit a Ref to a Value written prior to a previous Commit operation. If the r/w cache failed to persist across Commit() calls, then the below would have a validation failure.
	_, err = suite.db.CommitValue(ds, r)
	suite.NoError(err)
}

func (suite *DatabaseSuite) TestWriteRefToNonexistentValue() {
	suite.Panics(func() { suite.db.WriteValue(types.NewRef(types.Bool(true))) })
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.db.ReadValue(hash.Hash{}))
}

func (suite *DatabaseSuite) TestCommitProperlyTracksRoot() {
	id1, id2 := "testdataset", "othertestdataset"

	db1 := suite.makeDb(suite.cs)
	defer db1.Close()
	ds1 := db1.GetDataset(id1)
	ds1HeadVal := types.String("Commit value for " + id1)
	ds1, err := db1.CommitValue(ds1, ds1HeadVal)
	suite.NoError(err)

	db2 := suite.makeDb(suite.cs)
	defer db2.Close()
	ds2 := db2.GetDataset(id2)
	db2HeadVal := types.String("Commit value for " + id2)
	ds2, err = db2.CommitValue(ds2, db2HeadVal)
	suite.NoError(err)

	suite.EqualValues(ds1HeadVal, ds1.HeadValue())
	suite.EqualValues(db2HeadVal, ds2.HeadValue())
	suite.False(ds2.HeadValue().Equals(ds1HeadVal))
	suite.False(ds1.HeadValue().Equals(db2HeadVal))

	suite.Equal("tcu8fn066i70qi99pkd5u3gq0lqncek7", suite.cs.Root().String())
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "ds1"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	ds := suite.db.GetDataset(datasetID)
	a := types.String("a")
	ds2, err := suite.db.CommitValue(ds, a)
	suite.NoError(err)

	// ds2 matches the Datasets Map in suite.db
	suite.True(ds2.HeadRef().Equals(suite.db.GetDataset(datasetID).HeadRef()))

	// ds2 has |a| at its head
	h, ok := ds2.MaybeHeadValue()
	suite.True(ok)
	suite.True(h.Equals(a))
	suite.Equal(uint64(1), ds2.HeadRef().Height())

	ds = ds2
	aCommitRef := ds.HeadRef() // to be used to test disallowing of non-fastforward commits below

	// |a| <- |b|
	b := types.String("b")
	ds, err = suite.db.CommitValue(ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
	suite.Equal(uint64(2), ds.HeadRef().Height())

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.String("c")
	ds, err = suite.db.Commit(ds, c, newOpts(aCommitRef))
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(b))

	// |a| <- |b| <- |d|
	d := types.String("d")
	ds, err = suite.db.CommitValue(ds, d)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(d))
	suite.Equal(uint64(3), ds.HeadRef().Height())

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	ds, err = suite.db.Commit(ds, b, newOpts(aCommitRef))
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.db.CommitValue(suite.db.GetDataset("otherDS"), a)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDB := suite.makeDb(suite.cs)
	defer newDB.Close()
	datasets2 := newDB.Datasets()
	suite.Equal(uint64(2), datasets2.Len())
}

func newOpts(parents ...types.Value) CommitOptions {
	return CommitOptions{Parents: types.NewSet(parents...)}
}

func (suite *DatabaseSuite) TestDatabaseDuplicateCommit() {
	datasetID := "ds1"
	ds := suite.db.GetDataset(datasetID)
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	v := types.String("Hello")
	_, err := suite.db.CommitValue(ds, v)
	suite.NoError(err)

	_, err = suite.db.CommitValue(ds, v)
	suite.IsType(ErrMergeNeeded, err)
}

func (suite *DatabaseSuite) TestDatabaseCommitMerge() {
	datasetID1, datasetID2 := "ds1", "ds2"
	ds1, ds2 := suite.db.GetDataset(datasetID1), suite.db.GetDataset(datasetID2)

	var err error
	v := types.NewMap(types.String("Hello"), types.Number(42))
	ds1, err = suite.db.CommitValue(ds1, v)
	ds1First := ds1
	suite.NoError(err)
	ds1, err = suite.db.CommitValue(ds1, v.Set(types.String("Friends"), types.Bool(true)))
	suite.NoError(err)

	ds2, err = suite.db.CommitValue(ds2, types.String("Goodbye"))
	suite.NoError(err)

	// No common ancestor
	_, err = suite.db.Commit(ds1, types.Number(47), newOpts(ds2.HeadRef()))
	suite.IsType(ErrMergeNeeded, err, "%s", err)

	// Unmergeable
	_, err = suite.db.Commit(ds1, types.Number(47), newOptsWithMerge(merge.None, ds1First.HeadRef()))
	suite.IsType(&merge.ErrMergeConflict{}, err, "%s", err)

	// Merge policies
	newV := v.Set(types.String("Friends"), types.Bool(false))
	_, err = suite.db.Commit(ds1, newV, newOptsWithMerge(merge.None, ds1First.HeadRef()))
	suite.IsType(&merge.ErrMergeConflict{}, err, "%s", err)

	theirs, err := suite.db.Commit(ds1, newV, newOptsWithMerge(merge.Theirs, ds1First.HeadRef()))
	suite.NoError(err)
	suite.True(types.Bool(true).Equals(theirs.HeadValue().(types.Map).Get(types.String("Friends"))))

	newV = v.Set(types.String("Friends"), types.Number(47))
	ours, err := suite.db.Commit(ds1First, newV, newOptsWithMerge(merge.Ours, ds1First.HeadRef()))
	suite.NoError(err)
	suite.True(types.Number(47).Equals(ours.HeadValue().(types.Map).Get(types.String("Friends"))))
}

func newOptsWithMerge(policy merge.ResolveFunc, parents ...types.Value) CommitOptions {
	return CommitOptions{Parents: types.NewSet(parents...), Policy: merge.NewThreeWay(policy)}
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	ds1, ds2 := suite.db.GetDataset(datasetID1), suite.db.GetDataset(datasetID2)
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// ds1: |a|
	var err error
	a := types.String("a")
	ds1, err = suite.db.CommitValue(ds1, a)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(a))

	// ds1: |a|, ds2: |b|
	b := types.String("b")
	ds2, err = suite.db.CommitValue(ds2, b)
	suite.NoError(err)
	suite.True(ds2.HeadValue().Equals(b))

	ds1, err = suite.db.Delete(ds1)
	suite.NoError(err)
	suite.True(suite.db.GetDataset(datasetID2).HeadValue().Equals(b))
	_, present := suite.db.GetDataset(datasetID1).MaybeHead()
	suite.False(present, "Dataset %s should not be present", datasetID1)

	// Get a fresh database, and verify that only ds1 is present
	newDB := suite.makeDb(suite.cs)
	defer newDB.Close()
	datasets = newDB.Datasets()
	suite.Equal(uint64(1), datasets.Len())
	_, present = newDB.GetDataset(datasetID2).MaybeHeadRef()
	suite.True(present, "Dataset %s should be present", datasetID2)
}

type waitDuringUpdateRootChunkStore struct {
	chunks.ChunkStore
	preUpdateRootHook func()
}

func (w *waitDuringUpdateRootChunkStore) UpdateRoot(current, last hash.Hash) bool {
	if w.preUpdateRootHook != nil {
		w.preUpdateRootHook()
	}
	return w.ChunkStore.UpdateRoot(current, last)
}

func (suite *DatabaseSuite) TestCommitWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(datasetID)
	var err error

	// Setup:
	// ds1: |a| <- |b|
	ds1, err = suite.db.CommitValue(ds1, types.String("a"))
	b := types.String("b")
	ds1, err = suite.db.CommitValue(ds1, b)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	w := &waitDuringUpdateRootChunkStore{suite.cs, nil}
	db := suite.makeDb(w)
	defer db.Close()

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b|
	// ds2: |stuff|
	w.preUpdateRootHook = func() {
		e := types.String("stuff")
		ds2, concErr := suite.db.CommitValue(suite.db.GetDataset("ds2"), e)
		suite.NoError(concErr)
		suite.True(ds2.HeadValue().Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should proceed without a problem
	ds1 = db.GetDataset(datasetID)
	c := types.String("c")
	ds1, err = db.CommitValue(ds1, c)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(c))

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |c| <- |e|
	e := types.String("e")
	w.preUpdateRootHook = func() {
		ds := suite.db.GetDataset(datasetID)
		ds, concErr := suite.db.Commit(ds, e, CommitOptions{Parents: types.NewSet(ds1.HeadRef())})
		suite.NoError(concErr)
		suite.True(ds.HeadValue().Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should fail due to the above
	nope := types.String("nope")
	ds1, err = db.CommitValue(ds1, nope)
	suite.Error(err)
	v := ds1.HeadValue()
	suite.True(v.Equals(e), "%s", v.(types.String))
}

func (suite *DatabaseSuite) TestDeleteWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(datasetID)
	var err error

	// Setup:
	// ds1: |a| <- |b|
	ds1, err = suite.db.CommitValue(ds1, types.String("a"))
	b := types.String("b")
	ds1, err = suite.db.CommitValue(ds1, b)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	w := &waitDuringUpdateRootChunkStore{suite.cs, nil}
	db := suite.makeDb(w)
	defer db.Close()

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	w.preUpdateRootHook = func() {
		ds := suite.db.GetDataset(datasetID)
		ds, concErr := suite.db.Commit(ds, e, CommitOptions{Parents: types.NewSet(ds1.HeadRef())})
		suite.NoError(concErr)
		suite.True(ds.HeadValue().Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should fail due to the above
	ds1, err = db.Delete(ds1)
	suite.Error(err)
	suite.True(ds1.HeadValue().Equals(e))

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b| <- |e|
	// ds2: |stuff|
	w.preUpdateRootHook = func() {
		e := types.String("stuff")
		ds, concErr := suite.db.CommitValue(suite.db.GetDataset("other"), e)
		suite.NoError(concErr)
		suite.True(ds.HeadValue().Equals(e))
		w.preUpdateRootHook = nil
	}

	// Attempted Concurrent change, which should proceed without a problem
	ds1, err = db.Delete(ds1)
	suite.NoError(err)
	_, present := ds1.MaybeHeadRef()
	suite.False(present, "Dataset %s should not be present", datasetID)
}

func (suite *DatabaseSuite) TestSetHead() {
	var err error
	datasetID := "ds1"

	// |a| <- |b|
	ds := suite.db.GetDataset(datasetID)
	a := types.String("a")
	ds, err = suite.db.CommitValue(ds, a)
	suite.NoError(err)
	aCommitRef := ds.HeadRef() // To use in non-FF SetHead() below.

	b := types.String("b")
	ds, err = suite.db.CommitValue(ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
	bCommitRef := ds.HeadRef() // To use in FF SetHead() below.

	ds, err = suite.db.SetHead(ds, aCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(a))

	ds, err = suite.db.SetHead(ds, bCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
}

func (suite *DatabaseSuite) TestFastForward() {
	var err error
	datasetID := "ds1"

	// |a| <- |b| <- |c|
	ds := suite.db.GetDataset(datasetID)
	a := types.String("a")
	ds, err = suite.db.CommitValue(ds, a)
	suite.NoError(err)
	aCommitRef := ds.HeadRef() // To use in non-FF cases below.

	b := types.String("b")
	ds, err = suite.db.CommitValue(ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))

	c := types.String("c")
	ds, err = suite.db.CommitValue(ds, c)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(c))
	cCommitRef := ds.HeadRef() // To use in FastForward() below.

	// FastForward should disallow this, as |a| is not a descendant of |c|
	ds, err = suite.db.FastForward(ds, aCommitRef)
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(c))

	// Move Head back to something earlier in the lineage, so we can test FastForward
	ds, err = suite.db.SetHead(ds, aCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(a))

	// This should succeed, because while |a| is not a direct parent of |c|, it is an ancestor.
	ds, err = suite.db.FastForward(ds, cCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(c))
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

func (suite *DatabaseSuite) TestMetaOption() {
	ds := suite.db.GetDataset("ds1")
	m := types.NewStruct("M", types.StructData{
		"author": types.String("arv"),
	})

	ds, err := suite.db.Commit(ds, types.String("a"), CommitOptions{Meta: m})
	suite.NoError(err)
	c := ds.Head()
	suite.Equal(types.String("arv"), c.Get("meta").(types.Struct).Get("author"))
}
