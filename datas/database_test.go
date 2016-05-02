package datas

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
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
		hbs := newHTTPBatchStoreForTest(suite.cs)
		return &RemoteDatabaseClient{newDatabaseCommon(hbs, hbs)}
	}
	suite.db = suite.makeDb(suite.cs)
}

func (suite *DatabaseSuite) TearDownTest() {
	suite.db.Close()
	suite.cs.Close()
}

func (suite *DatabaseSuite) TestReadWriteCache() {
	var v types.Value = types.Bool(true)
	suite.NotEqual(ref.Ref{}, suite.db.WriteValue(v))
	r := suite.db.WriteValue(v).TargetRef()
	commit := NewCommit()
	newDb, err := suite.db.Commit("foo", commit.Set(ValueField, v))
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	v = newDb.ReadValue(r)
	suite.True(v.Equals(types.Bool(true)))
}

func (suite *DatabaseSuite) TestWriteRefToNonexistentValue() {
	suite.Panics(func() { suite.db.WriteValue(types.NewRef(types.Bool(true).Ref())) })
}

func (suite *DatabaseSuite) TestWriteWrongTypeRef() {
	b := types.Bool(true)
	blob := types.NewEmptyBlob()
	suite.NotEqual(ref.Ref{}, suite.db.WriteValue(b))

	suite.Panics(func() { suite.db.WriteValue(types.NewTypedRef(blob.Type(), b.Ref())) })
}

func (suite *DatabaseSuite) TestWriteValueTypeRef() {
	b := types.Bool(true)
	suite.NotEqual(ref.Ref{}, suite.db.WriteValue(b))

	suite.NotPanics(func() { suite.db.WriteValue(types.NewRef(b.Ref())) })
}

func (suite *DatabaseSuite) TestReadValueTypeRefPanics_BUG1121() {
	b := types.NewEmptyBlob()
	suite.NotEqual(ref.Ref{}, suite.db.WriteValue(b))

	datasetID := "db1"
	aCommit := NewCommit().Set(ValueField, types.NewRef(b.Ref()))
	db2, err := suite.db.Commit(datasetID, aCommit)
	suite.NoError(err)

	_, ok := db2.MaybeHead(datasetID)
	suite.True(ok)
	// Fix BUG 1121 and then uncomment this line and delete the one after
	// suite.NotPanics(func() { db2.WriteValue(types.NewRefOfBlob(b.Ref())) })
	suite.Panics(func() { db2.WriteValue(types.NewTypedRefFromValue(b)) })
}

func (suite *DatabaseSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.db.ReadValue(ref.Ref{}))
}

func (suite *DatabaseSuite) TestDatabaseCommit() {
	datasetID := "db1"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	db2, err := suite.db.Commit(datasetID, aCommit)
	suite.NoError(err)

	// The old database still has no head.
	_, ok := suite.db.MaybeHead(datasetID)
	suite.False(ok)

	// The new database has |a|.
	aCommit1 := db2.Head(datasetID)
	suite.True(aCommit1.Get(ValueField).Equals(a))
	suite.db = db2

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.NewString("c")
	cCommit := NewCommit().Set(ValueField, c)
	suite.db, err = suite.db.Commit(datasetID, cCommit)
	suite.Error(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b| <- |d|
	d := types.NewString("d")
	dCommit := NewCommit().Set(ValueField, d).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	suite.db, err = suite.db.Commit(datasetID, dCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(d))

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.Error(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.db.Commit("otherDb", aCommit)
	suite.NoError(err)

	// Get a fresh database, and verify that both datasets are present
	newDb := suite.makeDb(suite.cs)
	datasets2 := newDb.Datasets()
	suite.Equal(uint64(2), datasets2.Len())
	newDb.Close()
}

func (suite *DatabaseSuite) TestDatabaseDelete() {
	datasetID1, datasetID2 := "db1", "db2"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	var err error
	a := types.NewString("a")
	suite.db, err = suite.db.Commit(datasetID1, NewCommit().Set(ValueField, a))
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID1).Get(ValueField).Equals(a))

	// db1; |a|, db2: |b|
	b := types.NewString("b")
	suite.db, err = suite.db.Commit(datasetID2, NewCommit().Set(ValueField, b))
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID2).Get(ValueField).Equals(b))

	suite.db, err = suite.db.Delete(datasetID1)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID2).Get(ValueField).Equals(b))
	h, present := suite.db.MaybeHead(datasetID1)
	suite.False(present, "Dataset %s should not be present, but head is %v", datasetID1, h.Get(ValueField))

	// Get a fresh database, and verify that only db1 is present
	newDb := suite.makeDb(suite.cs)
	datasets = newDb.Datasets()
	suite.Equal(uint64(1), datasets.Len())
	_, present = suite.db.MaybeHead(datasetID2)
	suite.True(present, "Dataset %s should be present", datasetID2)
	newDb.Close()
}

func (suite *DatabaseSuite) TestDatabaseDeleteConcurrent() {
	datasetID := "db1"
	datasets := suite.db.Datasets()
	suite.Zero(datasets.Len())
	var err error

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	suite.NoError(err)

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	db2, err := suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(a))
	suite.True(db2.Head(datasetID).Get(ValueField).Equals(b))

	suite.db, err = suite.db.Delete(datasetID)
	suite.NoError(err)
	h, present := suite.db.MaybeHead(datasetID)
	suite.False(present, "Dataset %s should not be present, but head is %v", datasetID, h.Get(ValueField))
	h, present = db2.MaybeHead(datasetID)
	suite.True(present, "Dataset %s should be present", datasetID)

	// Get a fresh database, and verify that no databases are present
	newDb := suite.makeDb(suite.cs)
	datasets = newDb.Datasets()
	suite.Equal(uint64(0), datasets.Len())
	newDb.Close()
}

func (suite *DatabaseSuite) TestDatabaseConcurrency() {
	datasetID := "db1"
	var err error

	// Setup:
	// |a| <- |b|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	suite.db, err = suite.db.Commit(datasetID, aCommit)
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	suite.db, err = suite.db.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(b))

	// Important to create this here.
	db2 := suite.makeDb(suite.cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.NewString("c")
	cCommit := NewCommit().Set(ValueField, c).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	suite.db, err = suite.db.Commit(datasetID, cCommit)
	suite.NoError(err)
	suite.True(suite.db.Head(datasetID).Get(ValueField).Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, Database returned by Commit() should have |c| as Head.
	e := types.NewString("e")
	eCommit := NewCommit().Set(ValueField, e).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	db2, err = db2.Commit(datasetID, eCommit)
	suite.Error(err)
	suite.True(db2.Head(datasetID).Get(ValueField).Equals(c))
}
