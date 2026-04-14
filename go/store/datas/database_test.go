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
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestLocalDatabase(t *testing.T) {
	suite.Run(t, &LocalDatabaseSuite{})
}

func TestRemoteDatabase(t *testing.T) {
	suite.Run(t, &RemoteDatabaseSuite{})
}

type DatabaseSuite struct {
	suite.Suite
	storage *chunks.TestStorage
	db      *database
	makeDb  func(chunks.ChunkStore) Database
}

type LocalDatabaseSuite struct {
	DatabaseSuite
}

func (suite *LocalDatabaseSuite) SetupTest() {
	suite.storage = &chunks.TestStorage{}
	suite.makeDb = NewDatabase
	suite.db = suite.makeDb(suite.storage.NewViewWithDefaultFormat()).(*database)
}

type RemoteDatabaseSuite struct {
	DatabaseSuite
}

func (suite *RemoteDatabaseSuite) SetupTest() {
	suite.storage = &chunks.TestStorage{}
	suite.makeDb = func(cs chunks.ChunkStore) Database {
		return NewDatabase(cs)
	}
	suite.db = suite.makeDb(suite.storage.NewViewWithDefaultFormat()).(*database)
}

func (suite *DatabaseSuite) TearDownTest() {
	suite.db.Close()
}

func (suite *RemoteDatabaseSuite) TestWriteRefToNonexistentValue() {
	ds, err := suite.db.GetDataset(context.Background(), "foo")
	suite.NoError(err)
	r, err := types.NewRef(types.Bool(true), suite.db.Format())
	suite.NoError(err)
	_, err = CommitValue(context.Background(), suite.db, ds, r)
	suite.Error(err)
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.db.ReadValue(context.Background(), hash.Hash{}))
}

func (suite *DatabaseSuite) TestRebase() {
	datasetID := "ds1"
	ds1, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = CommitValue(context.Background(), suite.db, ds1, types.String("a"))
	b := types.String("b")
	ds1, err = CommitValue(context.Background(), suite.db, ds1, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds1).Equals(b))

	interloper := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer interloper.Close()

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	ds, err := interloper.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	iDS, concErr := CommitValue(context.Background(), interloper, ds, e)
	suite.NoError(concErr)
	suite.True(mustHeadValue(iDS).Equals(e))

	// suite.ds shouldn't see the above change yet
	ds, err = suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(b))

	err = ChunkStoreFromDatabase(suite.db).Rebase(context.Background())
	suite.NoError(err)
	ds, err = suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(e))
}

func (suite *DatabaseSuite) TestCommitProperlyTracksRoot() {
	id1, id2 := "testdataset", "othertestdataset"

	db1 := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer db1.Close()
	ds1, err := db1.GetDataset(context.Background(), id1)
	suite.NoError(err)
	ds1HeadVal := types.String("Commit value for " + id1)
	ds1, err = CommitValue(context.Background(), db1, ds1, ds1HeadVal)
	suite.NoError(err)

	db2 := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer db2.Close()
	ds2, err := db2.GetDataset(context.Background(), id2)
	suite.NoError(err)
	ds2HeadVal := types.String("Commit value for " + id2)
	ds2, err = CommitValue(context.Background(), db2, ds2, ds2HeadVal)
	suite.NoError(err)

	suite.EqualValues(ds1HeadVal, mustHeadValue(ds1))
	suite.EqualValues(ds2HeadVal, mustHeadValue(ds2))
	suite.False(mustHeadValue(ds2).Equals(ds1HeadVal))
	suite.False(mustHeadValue(ds1).Equals(ds2HeadVal))
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "ds1"
	datasets, err := suite.db.Datasets(context.Background())
	suite.NoError(err)
	suite.Zero(datasets.Len())

	// |a|
	ds, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	a := types.String("a")
	ds2, err := CommitValue(context.Background(), suite.db, ds, a)
	suite.NoError(err)

	// ds2 matches the Datasets Map in suite.db
	suiteDS, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	headAddr := mustHeadAddr(suiteDS)
	suite.True(mustHeadAddr(ds2) == headAddr)

	// ds2 has |a| at its head
	h, ok, err := ds2.MaybeHeadValue()
	suite.NoError(err)
	suite.True(ok)
	suite.True(h.Equals(a))
	comm, err := CommitFromValue(suite.db.Format(), mustHead(ds2))
	suite.NoError(err)
	suite.Equal(uint64(1), comm.Height())

	ds = ds2
	aCommitAddr := mustHeadAddr(ds) // to be used to test disallowing of non-fastforward commits below

	// |a| <- |b|
	b := types.String("b")
	ds, err = CommitValue(context.Background(), suite.db, ds, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(b))
	comm, err = CommitFromValue(suite.db.Format(), mustHead(ds))
	suite.NoError(err)
	suite.Equal(uint64(2), comm.Height())

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.String("c")
	_, err = suite.db.Commit(context.Background(), ds, c, newOpts(suite.db, aCommitAddr))
	suite.Error(err)
	suite.True(mustHeadValue(ds).Equals(b))

	// |a| <- |b| <- |d|
	d := types.String("d")
	ds, err = CommitValue(context.Background(), suite.db, ds, d)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(d))
	comm, err = CommitFromValue(suite.db.Format(), mustHead(ds))
	suite.NoError(err)
	suite.Equal(uint64(3), comm.Height())

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	_, err = suite.db.Commit(context.Background(), ds, b, newOpts(suite.db, aCommitAddr))
	suite.Error(err)
	suite.True(mustHeadValue(ds).Equals(d))

	// Add a commit to a different datasetId
	ds, err = suite.db.GetDataset(context.Background(), "otherDS")
	suite.NoError(err)
	_, err = CommitValue(context.Background(), suite.db, ds, a)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDB := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer newDB.Close()
	datasets2, err := newDB.Datasets(context.Background())
	suite.NoError(err)
	l, err := datasets2.Len()
	suite.NoError(err)
	suite.Equal(uint64(2), l)
}

func zeroMeta() *CommitMeta {
	epoch := CommitDateAt(time.UnixMilli(0))
	return &CommitMeta{
		Author:    CommitIdent{Date: epoch},
		Committer: CommitIdent{Date: epoch},
	}
}

func newOpts(vrw types.ValueReadWriter, parent hash.Hash) CommitOptions {
	return CommitOptions{Parents: []hash.Hash{parent}, Meta: zeroMeta()}
}

func (suite *DatabaseSuite) TestDatabaseDuplicateCommit() {
	datasetID := "ds1"
	ds, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	datasets, err := suite.db.Datasets(context.Background())
	suite.NoError(err)
	suite.Zero(datasets.Len())

	v := types.String("Hello")
	_, err = CommitValue(context.Background(), suite.db, ds, v)
	suite.NoError(err)

	_, err = CommitValue(context.Background(), suite.db, ds, v)
	suite.IsType(ErrMergeNeeded, err)
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	ds1, err := suite.db.GetDataset(context.Background(), datasetID1)
	suite.NoError(err)
	ds2, err := suite.db.GetDataset(context.Background(), datasetID2)
	suite.NoError(err)
	datasets, err := suite.db.Datasets(context.Background())
	suite.NoError(err)
	suite.Zero(datasets.Len())

	// ds1: |a|
	a := types.String("a")
	ds1, err = CommitValue(context.Background(), suite.db, ds1, a)
	suite.NoError(err)
	suite.True(mustHeadValue(ds1).Equals(a))

	// ds1: |a|, ds2: |b|
	b := types.String("b")
	ds2, err = CommitValue(context.Background(), suite.db, ds2, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds2).Equals(b))

	ds1, err = suite.db.Delete(context.Background(), ds1, "")
	suite.NoError(err)
	currDS2, err := suite.db.GetDataset(context.Background(), datasetID2)
	suite.NoError(err)
	suite.True(mustHeadValue(currDS2).Equals(b))
	currDS1, err := suite.db.GetDataset(context.Background(), datasetID1)
	suite.NoError(err)
	_, present := currDS1.MaybeHead()
	suite.False(present, "Dataset %s should not be present", datasetID1)

	// Get a fresh database, and verify that only ds2 is present
	newDB := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer newDB.Close()
	datasets, err = newDB.Datasets(context.Background())
	suite.NoError(err)
	l, err := datasets.Len()
	suite.NoError(err)
	suite.Equal(uint64(1), l)
	newDS, err := newDB.GetDataset(context.Background(), datasetID2)
	suite.NoError(err)
	present = newDS.HasHead()
	suite.True(present, "Dataset %s should be present", datasetID2)
}

func (suite *DatabaseSuite) TestCommitWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = CommitValue(context.Background(), suite.db, ds1, types.String("a"))
	b := types.String("b")
	ds1, err = CommitValue(context.Background(), suite.db, ds1, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds1).Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	interloperCS := suite.storage.NewViewWithDefaultFormat()
	interloper := suite.makeDb(interloperCS)
	defer interloper.Close()

	// Change ds2 behind suite.db's back. This shouldn't block changes to ds1 via suite.db below.
	// ds1: |a| <- |b|
	// ds2: |stuff|
	stf := types.String("stuff")
	ds2, err := interloper.GetDataset(context.Background(), "ds2")
	suite.NoError(err)
	ds2, concErr := CommitValue(context.Background(), interloper, ds2, stf)
	suite.NoError(concErr)
	suite.True(mustHeadValue(ds2).Equals(stf))

	// Change ds1 via suite.db, which should proceed without a problem
	c := types.String("c")
	ds1, err = CommitValue(context.Background(), suite.db, ds1, c)
	suite.NoError(err)
	suite.True(mustHeadValue(ds1).Equals(c))

	// Change ds1 behind suite.db's back. Will block changes to ds1 below.
	// ds1: |a| <- |b| <- |c| <- |e|
	e := types.String("e")
	interloperCS.Rebase(context.Background())
	iDS, err := interloper.GetDataset(context.Background(), "ds1")
	suite.NoError(err)
	iDS, concErr = CommitValue(context.Background(), interloper, iDS, e)
	suite.NoError(concErr)
	suite.True(mustHeadValue(iDS).Equals(e))
	v := mustHeadValue(iDS)
	suite.True(v.Equals(e), "%s", v.(types.String))

	// Attempted Concurrent change, which should fail due to the above
	nope := types.String("nope")
	_, err = CommitValue(context.Background(), suite.db, ds1, nope)
	suite.Error(err)
}

func (suite *DatabaseSuite) TestDeleteWithConcurrentChunkStoreUse() {
	datasetID := "ds1"
	ds1, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.Require().NoError(err)

	// Setup:
	// ds1: |a| <- |b|
	ds1, _ = CommitValue(context.Background(), suite.db, ds1, types.String("a"))
	b := types.String("b")
	ds1, err = CommitValue(context.Background(), suite.db, ds1, b)
	suite.Require().NoError(err)
	suite.Require().True(mustHeadValue(ds1).Equals(b))

	// Craft DB that will allow me to move the backing ChunkStore while suite.db isn't looking
	interloper := suite.makeDb(suite.storage.NewViewWithDefaultFormat())
	defer interloper.Close()

	// Concurrent change, to move root out from under my feet:
	// ds1: |a| <- |b| <- |e|
	e := types.String("e")
	iDS, err := interloper.GetDataset(context.Background(), datasetID)
	suite.Require().NoError(err)
	iDS, concErr := CommitValue(context.Background(), interloper, iDS, e)
	suite.Require().NoError(concErr)
	suite.Require().True(mustHeadValue(iDS).Equals(e))

	// Attempt to delete ds1 via suite.db, which should fail due to the above
	_, err = suite.db.Delete(context.Background(), ds1, "")
	suite.Require().Error(err)

	// Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
	// ds1: |a| <- |b| <- |e|
	// ds2: |stuff|
	stf := types.String("stuff")
	otherDS, err := suite.db.GetDataset(context.Background(), "other")
	suite.Require().NoError(err)
	iDS, concErr = CommitValue(context.Background(), suite.db, otherDS, stf)
	suite.Require().NoError(concErr)
	suite.Require().True(mustHeadValue(iDS).Equals(stf))

	// Attempted concurrent delete, which should proceed without a problem
	ds1, err = suite.db.Delete(context.Background(), ds1, "")
	suite.Require().NoError(err)
	present := ds1.HasHead()
	suite.False(present, "Dataset %s should not be present", datasetID)
}

func (suite *DatabaseSuite) TestSetHead() {
	var err error
	datasetID := "ds1"

	// |a| <- |b|
	ds, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	a := types.String("a")
	ds, err = CommitValue(context.Background(), suite.db, ds, a)
	suite.NoError(err)
	aCommitAddr := mustHeadAddr(ds) // To use in non-FF SetHeadToCommit() below.

	b := types.String("b")
	ds, err = CommitValue(context.Background(), suite.db, ds, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(b))
	bCommitAddr := mustHeadAddr(ds) // To use in FF SetHeadToCommit() below.

	ds, err = suite.db.SetHead(context.Background(), ds, aCommitAddr, "")
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(a))

	ds, err = suite.db.SetHead(context.Background(), ds, bCommitAddr, "")
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(b))
}

func (suite *DatabaseSuite) TestFastForward() {
	datasetID := "ds1"

	// |a| <- |b| <- |c|
	ds, err := suite.db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	a := types.String("a")
	ds, err = CommitValue(context.Background(), suite.db, ds, a)
	suite.NoError(err)
	aCommitAddr := mustHeadAddr(ds) // To use in non-FF cases below.

	b := types.String("b")
	ds, err = CommitValue(context.Background(), suite.db, ds, b)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(b))

	c := types.String("c")
	ds, err = CommitValue(context.Background(), suite.db, ds, c)
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(c))
	cCommitAddr := mustHeadAddr(ds) // To use in FastForward() below.

	// FastForward should disallow this, as |a| is not a descendant of |c|
	_, err = suite.db.FastForward(context.Background(), ds, aCommitAddr, "", false)
	suite.Error(err)

	// Move Head back to something earlier in the lineage, so we can test FastForward
	ds, err = suite.db.SetHead(context.Background(), ds, aCommitAddr, "")
	suite.NoError(err)
	suite.True(mustHeadValue(ds).Equals(a))

	// This should succeed, because while |a| is not a direct parent of |c|, it is an ancestor.
	ds, err = suite.db.FastForward(context.Background(), ds, cCommitAddr, "", false)
	suite.Require().NoError(err)
	suite.True(mustHeadValue(ds).Equals(c))
}

func (suite *DatabaseSuite) TestDatabaseHeightOfRefs() {
	r1, err := suite.db.WriteValue(context.Background(), types.String("hello"))
	suite.NoError(err)
	suite.Equal(uint64(1), r1.Height())

	r2, err := suite.db.WriteValue(context.Background(), r1)
	suite.NoError(err)
	suite.Equal(uint64(2), r2.Height())
	suite.Equal(uint64(3), mustRef(suite.db.WriteValue(context.Background(), r2)).Height())
}

func (suite *DatabaseSuite) TestMetaOption() {
	ctx := context.Background()
	ds, err := suite.db.GetDataset(ctx, "ds1")
	suite.NoError(err)

	ds, err = suite.db.Commit(ctx, ds, types.String("a"), CommitOptions{Meta: &CommitMeta{Author: CommitIdent{Name: "arv"}}})
	suite.NoError(err)
	meta, err := GetCommitMeta(ctx, mustHead(ds))
	suite.Equal("arv", meta.Author.Name)
}
