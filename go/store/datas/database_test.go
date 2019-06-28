// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/merge"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestLocalDatabase(t *testing.T) {
	suite.Run(t, &LocalDatabaseSuite{})
}

func TestRemoteDatabase(t *testing.T) {
	suite.Run(t, &RemoteDatabaseSuite{})
}

func TestValidateRef(t *testing.T) {
	st := &chunks.TestStorage{}
	db := NewDatabase(st.NewView()).(*database)
	defer db.Close()
	b := types.Bool(true)
	r := db.WriteValue(context.Background(), b)

	assert.Panics(t, func() { db.validateRefAsCommit(context.Background(), r) })
	assert.Panics(t, func() { db.validateRefAsCommit(context.Background(), types.NewRef(b, types.Format_7_18)) })
}

type DatabaseSuite struct {
	suite.Suite
	storage *chunks.TestStorage
	db      Database
	makeDb  func(chunks.ChunkStore) Database
}

type LocalDatabaseSuite struct {
	DatabaseSuite
}

func (suite *LocalDatabaseSuite) SetupTest() {
	suite.storage = &chunks.TestStorage{}
	suite.makeDb = NewDatabase
	suite.db = suite.makeDb(suite.storage.NewView())
}

type RemoteDatabaseSuite struct {
	DatabaseSuite
}

func (suite *RemoteDatabaseSuite) SetupTest() {
	suite.storage = &chunks.TestStorage{}
	suite.makeDb = func(cs chunks.ChunkStore) Database {
		return NewDatabase(cs)
	}
	suite.db = suite.makeDb(suite.storage.NewView())
}

func (suite *DatabaseSuite) TearDownTest() {
	suite.db.Close()
}

func (suite *RemoteDatabaseSuite) TestWriteRefToNonexistentValue() {
	ds := suite.db.GetDataset(context.Background(), "foo")
	r := types.NewRef(types.Bool(true), types.Format_7_18)
	suite.Panics(func() { suite.db.CommitValue(context.Background(), ds, r) })
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.db.ReadValue(context.Background(), hash.Hash{}))
}

func (suite *DatabaseSuite) TestCompletenessCheck() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(context.Background(), datasetID)

	se := types.NewSet(context.Background(), suite.db).Edit()
	for i := 0; i < 100; i++ {
		se.Insert(suite.db.WriteValue(context.Background(), types.Float(100)))
	}
	s := se.Set(context.Background())

	ds1, err := suite.db.CommitValue(context.Background(), ds1, s)
	suite.NoError(err)

	s = ds1.HeadValue().(types.Set)
	s = s.Edit().Insert(types.NewRef(types.Float(1000), types.Format_7_18)).Set(context.Background()) // danging ref
	suite.Panics(func() {
		ds1, err = suite.db.CommitValue(context.Background(), ds1, s)
	})
}

func (suite *DatabaseSuite) TestRebase() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(context.Background(), datasetID)
	var err error

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = suite.db.CommitValue(context.Background(), ds1, types.String("a"))
	b := types.String("b")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, b)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(b))

	interloper := suite.makeDb(suite.storage.NewView())
	defer interloper.Close()

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	iDS, concErr := interloper.CommitValue(context.Background(), interloper.GetDataset(context.Background(), datasetID), e)
	suite.NoError(concErr)
	suite.True(iDS.HeadValue().Equals(e))

	// suite.ds shouldn't see the above change yet
	suite.True(suite.db.GetDataset(context.Background(), datasetID).HeadValue().Equals(b))

	suite.db.Rebase(context.Background())
	suite.True(suite.db.GetDataset(context.Background(), datasetID).HeadValue().Equals(e))

	cs := suite.storage.NewView()
	noChangeDB := suite.makeDb(cs)
	noChangeDB.Datasets(context.Background())
	cs.Reads = 0 // New baseline

	noChangeDB.Rebase(context.Background())
	suite.Zero(cs.Reads)
}

func (suite *DatabaseSuite) TestCommitProperlyTracksRoot() {
	id1, id2 := "testdataset", "othertestdataset"

	db1 := suite.makeDb(suite.storage.NewView())
	defer db1.Close()
	ds1 := db1.GetDataset(context.Background(), id1)
	ds1HeadVal := types.String("Commit value for " + id1)
	ds1, err := db1.CommitValue(context.Background(), ds1, ds1HeadVal)
	suite.NoError(err)

	db2 := suite.makeDb(suite.storage.NewView())
	defer db2.Close()
	ds2 := db2.GetDataset(context.Background(), id2)
	ds2HeadVal := types.String("Commit value for " + id2)
	ds2, err = db2.CommitValue(context.Background(), ds2, ds2HeadVal)
	suite.NoError(err)

	suite.EqualValues(ds1HeadVal, ds1.HeadValue())
	suite.EqualValues(ds2HeadVal, ds2.HeadValue())
	suite.False(ds2.HeadValue().Equals(ds1HeadVal))
	suite.False(ds1.HeadValue().Equals(ds2HeadVal))
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "ds1"
	datasets := suite.db.Datasets(context.Background())
	suite.Zero(datasets.Len())

	// |a|
	ds := suite.db.GetDataset(context.Background(), datasetID)
	a := types.String("a")
	ds2, err := suite.db.CommitValue(context.Background(), ds, a)
	suite.NoError(err)

	// ds2 matches the Datasets Map in suite.db
	suite.True(ds2.HeadRef().Equals(suite.db.GetDataset(context.Background(), datasetID).HeadRef()))

	// ds2 has |a| at its head
	h, ok := ds2.MaybeHeadValue()
	suite.True(ok)
	suite.True(h.Equals(a))
	suite.Equal(uint64(1), ds2.HeadRef().Height())

	ds = ds2
	aCommitRef := ds.HeadRef() // to be used to test disallowing of non-fastforward commits below

	// |a| <- |b|
	b := types.String("b")
	ds, err = suite.db.CommitValue(context.Background(), ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
	suite.Equal(uint64(2), ds.HeadRef().Height())

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.String("c")
	ds, err = suite.db.Commit(context.Background(), ds, c, newOpts(suite.db, aCommitRef))
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(b))

	// |a| <- |b| <- |d|
	d := types.String("d")
	ds, err = suite.db.CommitValue(context.Background(), ds, d)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(d))
	suite.Equal(uint64(3), ds.HeadRef().Height())

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	ds, err = suite.db.Commit(context.Background(), ds, b, newOpts(suite.db, aCommitRef))
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.db.CommitValue(context.Background(), suite.db.GetDataset(context.Background(), "otherDS"), a)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDB := suite.makeDb(suite.storage.NewView())
	defer newDB.Close()
	datasets2 := newDB.Datasets(context.Background())
	suite.Equal(uint64(2), datasets2.Len())
}

func (suite *DatabaseSuite) TestDatasetsMapType() {
	dsID1, dsID2 := "ds1", "ds2"

	datasets := suite.db.Datasets(context.Background())
	ds, err := suite.db.CommitValue(context.Background(), suite.db.GetDataset(context.Background(), dsID1), types.String("a"))
	suite.NoError(err)
	suite.NotPanics(func() {
		assertMapOfStringToRefOfCommit(context.Background(), suite.db.Datasets(context.Background()), datasets, suite.db)
	})

	datasets = suite.db.Datasets(context.Background())
	_, err = suite.db.CommitValue(context.Background(), suite.db.GetDataset(context.Background(), dsID2), types.Float(42))
	suite.NoError(err)
	suite.NotPanics(func() {
		assertMapOfStringToRefOfCommit(context.Background(), suite.db.Datasets(context.Background()), datasets, suite.db)
	})

	datasets = suite.db.Datasets(context.Background())
	_, err = suite.db.Delete(context.Background(), ds)
	suite.NoError(err)
	suite.NotPanics(func() {
		assertMapOfStringToRefOfCommit(context.Background(), suite.db.Datasets(context.Background()), datasets, suite.db)
	})
}

func assertMapOfStringToRefOfCommit(ctx context.Context, proposed, datasets types.Map, vr types.ValueReader) {
	stopChan := make(chan struct{})
	defer close(stopChan)
	changes := make(chan types.ValueChanged)
	go func() {
		defer close(changes)
		proposed.Diff(ctx, datasets, changes, stopChan)
	}()
	for change := range changes {
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			// Since this is a Map Diff, change.V is the key at which a change was detected.
			// Go get the Value there, which should be a Ref<Value>, deref it, and then ensure the target is a Commit.
			val := change.NewValue
			ref, ok := val.(types.Ref)
			if !ok {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, but key %s maps to a %s", change.Key.(types.String), types.TypeOf(val).Describe(ctx))
			}
			if targetValue := ref.TargetValue(ctx, vr); !IsCommit(targetValue) {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, but the ref at key %s points to a %s", change.Key.(types.String), types.TypeOf(targetValue).Describe(ctx))
			}
		}
	}
}

func newOpts(vrw types.ValueReadWriter, parents ...types.Value) CommitOptions {
	return CommitOptions{Parents: types.NewSet(context.Background(), vrw, parents...)}
}

func (suite *DatabaseSuite) TestDatabaseDuplicateCommit() {
	datasetID := "ds1"
	ds := suite.db.GetDataset(context.Background(), datasetID)
	datasets := suite.db.Datasets(context.Background())
	suite.Zero(datasets.Len())

	v := types.String("Hello")
	_, err := suite.db.CommitValue(context.Background(), ds, v)
	suite.NoError(err)

	_, err = suite.db.CommitValue(context.Background(), ds, v)
	suite.IsType(ErrMergeNeeded, err)
}

func (suite *DatabaseSuite) TestDatabaseCommitMerge() {
	datasetID1, datasetID2 := "ds1", "ds2"
	ds1, ds2 := suite.db.GetDataset(context.Background(), datasetID1), suite.db.GetDataset(context.Background(), datasetID2)

	var err error
	v := types.NewMap(context.Background(), types.Format_7_18, suite.db, types.String("Hello"), types.Float(42))
	ds1, err = suite.db.CommitValue(context.Background(), ds1, v)
	ds1First := ds1
	suite.NoError(err)
	ds1, err = suite.db.CommitValue(context.Background(), ds1, v.Edit().Set(types.String("Friends"), types.Bool(true)).Map(context.Background()))
	suite.NoError(err)

	ds2, err = suite.db.CommitValue(context.Background(), ds2, types.String("Goodbye"))
	suite.NoError(err)

	// No common ancestor
	_, err = suite.db.Commit(context.Background(), ds1, types.Float(47), newOpts(suite.db, ds2.HeadRef()))
	suite.IsType(ErrMergeNeeded, err, "%s", err)

	// Unmergeable
	_, err = suite.db.Commit(context.Background(), ds1, types.Float(47), newOptsWithMerge(suite.db, merge.None, ds1First.HeadRef()))
	suite.IsType(&merge.ErrMergeConflict{}, err, "%s", err)

	// Merge policies
	newV := v.Edit().Set(types.String("Friends"), types.Bool(false)).Map(context.Background())
	_, err = suite.db.Commit(context.Background(), ds1, newV, newOptsWithMerge(suite.db, merge.None, ds1First.HeadRef()))
	suite.IsType(&merge.ErrMergeConflict{}, err, "%s", err)

	theirs, err := suite.db.Commit(context.Background(), ds1, newV, newOptsWithMerge(suite.db, merge.Theirs, ds1First.HeadRef()))
	suite.NoError(err)
	suite.True(types.Bool(true).Equals(theirs.HeadValue().(types.Map).Get(context.Background(), types.String("Friends"))))

	newV = v.Edit().Set(types.String("Friends"), types.Float(47)).Map(context.Background())
	ours, err := suite.db.Commit(context.Background(), ds1First, newV, newOptsWithMerge(suite.db, merge.Ours, ds1First.HeadRef()))
	suite.NoError(err)
	suite.True(types.Float(47).Equals(ours.HeadValue().(types.Map).Get(context.Background(), types.String("Friends"))))
}

func newOptsWithMerge(vrw types.ValueReadWriter, policy merge.ResolveFunc, parents ...types.Value) CommitOptions {
	return CommitOptions{Parents: types.NewSet(context.Background(), vrw, parents...), Policy: merge.NewThreeWay(policy)}
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	ds1, ds2 := suite.db.GetDataset(context.Background(), datasetID1), suite.db.GetDataset(context.Background(), datasetID2)
	datasets := suite.db.Datasets(context.Background())
	suite.Zero(datasets.Len())

	// ds1: |a|
	var err error
	a := types.String("a")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, a)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(a))

	// ds1: |a|, ds2: |b|
	b := types.String("b")
	ds2, err = suite.db.CommitValue(context.Background(), ds2, b)
	suite.NoError(err)
	suite.True(ds2.HeadValue().Equals(b))

	ds1, err = suite.db.Delete(context.Background(), ds1)
	suite.NoError(err)
	suite.True(suite.db.GetDataset(context.Background(), datasetID2).HeadValue().Equals(b))
	_, present := suite.db.GetDataset(context.Background(), datasetID1).MaybeHead()
	suite.False(present, "Dataset %s should not be present", datasetID1)

	// Get a fresh database, and verify that only ds2 is present
	newDB := suite.makeDb(suite.storage.NewView())
	defer newDB.Close()
	datasets = newDB.Datasets(context.Background())
	suite.Equal(uint64(1), datasets.Len())
	_, present = newDB.GetDataset(context.Background(), datasetID2).MaybeHeadRef()
	suite.True(present, "Dataset %s should be present", datasetID2)
}

func (suite *DatabaseSuite) TestCommitWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(context.Background(), datasetID)
	var err error

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = suite.db.CommitValue(context.Background(), ds1, types.String("a"))
	b := types.String("b")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, b)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	interloper := suite.makeDb(suite.storage.NewView())
	defer interloper.Close()

	// Change ds2 behind suite.db's back. This shouldn't block changes to ds1 via suite.db below.
	// ds1: |a| <- |b|
	// ds2: |stuff|
	stf := types.String("stuff")
	ds2, concErr := interloper.CommitValue(context.Background(), interloper.GetDataset(context.Background(), "ds2"), stf)
	suite.NoError(concErr)
	suite.True(ds2.HeadValue().Equals(stf))

	// Change ds1 via suite.db, which should proceed without a problem
	c := types.String("c")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, c)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(c))

	// Change ds1 behind suite.db's back. Will block changes to ds1 below.
	// ds1: |a| <- |b| <- |c| <- |e|
	e := types.String("e")
	interloper.Rebase(context.Background())
	iDS, concErr := interloper.CommitValue(context.Background(), interloper.GetDataset(context.Background(), "ds1"), e)
	suite.NoError(concErr)
	suite.True(iDS.HeadValue().Equals(e))

	// Attempted Concurrent change, which should fail due to the above
	nope := types.String("nope")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, nope)
	suite.Error(err)
	v := ds1.HeadValue()
	suite.True(v.Equals(e), "%s", v.(types.String))
}

func (suite *DatabaseSuite) TestDeleteWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1 := suite.db.GetDataset(context.Background(), datasetID)
	var err error

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = suite.db.CommitValue(context.Background(), ds1, types.String("a"))
	b := types.String("b")
	ds1, err = suite.db.CommitValue(context.Background(), ds1, b)
	suite.NoError(err)
	suite.True(ds1.HeadValue().Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	interloper := suite.makeDb(suite.storage.NewView())
	defer interloper.Close()

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	iDS, concErr := interloper.CommitValue(context.Background(), interloper.GetDataset(context.Background(), datasetID), e)
	suite.NoError(concErr)
	suite.True(iDS.HeadValue().Equals(e))

	// Attempt to delete ds1 via suite.db, which should fail due to the above
	ds1, err = suite.db.Delete(context.Background(), ds1)
	suite.Error(err)
	suite.True(ds1.HeadValue().Equals(e))

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b| <- |e|
	// ds2: |stuff|
	stf := types.String("stuff")
	iDS, concErr = interloper.CommitValue(context.Background(), suite.db.GetDataset(context.Background(), "other"), stf)
	suite.NoError(concErr)
	suite.True(iDS.HeadValue().Equals(stf))

	// Attempted concurrent delete, which should proceed without a problem
	ds1, err = suite.db.Delete(context.Background(), ds1)
	suite.NoError(err)
	_, present := ds1.MaybeHeadRef()
	suite.False(present, "Dataset %s should not be present", datasetID)
}

func (suite *DatabaseSuite) TestSetHead() {
	var err error
	datasetID := "ds1"

	// |a| <- |b|
	ds := suite.db.GetDataset(context.Background(), datasetID)
	a := types.String("a")
	ds, err = suite.db.CommitValue(context.Background(), ds, a)
	suite.NoError(err)
	aCommitRef := ds.HeadRef() // To use in non-FF SetHead() below.

	b := types.String("b")
	ds, err = suite.db.CommitValue(context.Background(), ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
	bCommitRef := ds.HeadRef() // To use in FF SetHead() below.

	ds, err = suite.db.SetHead(context.Background(), ds, aCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(a))

	ds, err = suite.db.SetHead(context.Background(), ds, bCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))
}

func (suite *DatabaseSuite) TestFastForward() {
	var err error
	datasetID := "ds1"

	// |a| <- |b| <- |c|
	ds := suite.db.GetDataset(context.Background(), datasetID)
	a := types.String("a")
	ds, err = suite.db.CommitValue(context.Background(), ds, a)
	suite.NoError(err)
	aCommitRef := ds.HeadRef() // To use in non-FF cases below.

	b := types.String("b")
	ds, err = suite.db.CommitValue(context.Background(), ds, b)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(b))

	c := types.String("c")
	ds, err = suite.db.CommitValue(context.Background(), ds, c)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(c))
	cCommitRef := ds.HeadRef() // To use in FastForward() below.

	// FastForward should disallow this, as |a| is not a descendant of |c|
	ds, err = suite.db.FastForward(context.Background(), ds, aCommitRef)
	suite.Error(err)
	suite.True(ds.HeadValue().Equals(c))

	// Move Head back to something earlier in the lineage, so we can test FastForward
	ds, err = suite.db.SetHead(context.Background(), ds, aCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(a))

	// This should succeed, because while |a| is not a direct parent of |c|, it is an ancestor.
	ds, err = suite.db.FastForward(context.Background(), ds, cCommitRef)
	suite.NoError(err)
	suite.True(ds.HeadValue().Equals(c))
}

func (suite *DatabaseSuite) TestDatabaseHeightOfRefs() {
	r1 := suite.db.WriteValue(context.Background(), types.String("hello"))
	suite.Equal(uint64(1), r1.Height())

	r2 := suite.db.WriteValue(context.Background(), r1)
	suite.Equal(uint64(2), r2.Height())
	suite.Equal(uint64(3), suite.db.WriteValue(context.Background(), r2).Height())
}

func (suite *DatabaseSuite) TestDatabaseHeightOfCollections() {
	setOfStringType := types.MakeSetType(types.StringType)
	setOfRefOfStringType := types.MakeSetType(types.MakeRefType(types.StringType))

	// Set<String>
	v1 := types.String("hello")
	v2 := types.String("world")
	s1 := types.NewSet(context.Background(), suite.db, v1, v2)
	suite.Equal(uint64(1), suite.db.WriteValue(context.Background(), s1).Height())

	// Set<Ref<String>>
	s2 := types.NewSet(context.Background(), suite.db, suite.db.WriteValue(context.Background(), v1), suite.db.WriteValue(context.Background(), v2))
	suite.Equal(uint64(2), suite.db.WriteValue(context.Background(), s2).Height())

	// List<Set<String>>
	v3 := types.String("foo")
	v4 := types.String("bar")
	s3 := types.NewSet(context.Background(), suite.db, v3, v4)
	l1 := types.NewList(context.Background(), types.Format_7_18, suite.db, s1, s3)
	suite.Equal(uint64(1), suite.db.WriteValue(context.Background(), l1).Height())

	// List<Ref<Set<String>>
	l2 := types.NewList(context.Background(), types.Format_7_18, suite.db, suite.db.WriteValue(context.Background(), s1), suite.db.WriteValue(context.Background(), s3))
	suite.Equal(uint64(2), suite.db.WriteValue(context.Background(), l2).Height())

	// List<Ref<Set<Ref<String>>>
	s4 := types.NewSet(context.Background(), suite.db, suite.db.WriteValue(context.Background(), v3), suite.db.WriteValue(context.Background(), v4))
	l3 := types.NewList(context.Background(), types.Format_7_18, suite.db, suite.db.WriteValue(context.Background(), s4))
	suite.Equal(uint64(3), suite.db.WriteValue(context.Background(), l3).Height())

	// List<Set<String> | RefValue<Set<String>>>
	l4 := types.NewList(context.Background(), types.Format_7_18, suite.db, s1, suite.db.WriteValue(context.Background(), s3))
	suite.Equal(uint64(2), suite.db.WriteValue(context.Background(), l4).Height())
	l5 := types.NewList(context.Background(), types.Format_7_18, suite.db, suite.db.WriteValue(context.Background(), s1), s3)
	suite.Equal(uint64(2), suite.db.WriteValue(context.Background(), l5).Height())

	// Familiar with the "New Jersey Turnpike" drink? Here's the noms version of that...
	everything := []types.Value{v1, v2, s1, s2, v3, v4, s3, l1, l2, s4, l3, l4, l5}
	andMore := make([]types.Value, 0, len(everything)*3+2)
	for _, v := range everything {
		andMore = append(andMore, v, types.TypeOf(v), suite.db.WriteValue(context.Background(), v))
	}
	andMore = append(andMore, setOfStringType, setOfRefOfStringType)

	suite.db.WriteValue(context.Background(), types.NewList(context.Background(), types.Format_7_18, suite.db, andMore...))
}

func (suite *DatabaseSuite) TestMetaOption() {
	ds := suite.db.GetDataset(context.Background(), "ds1")
	m := types.NewStruct("M", types.StructData{
		"author": types.String("arv"),
	})

	ds, err := suite.db.Commit(context.Background(), ds, types.String("a"), CommitOptions{Meta: m})
	suite.NoError(err)
	c := ds.Head()
	suite.Equal(types.String("arv"), c.Get("meta").(types.Struct).Get("author"))
}
