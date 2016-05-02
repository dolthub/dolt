package datas

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

// writesOnCommit allows tests to adjust for how many writes dataStoreCommon performs on Commit()
const writesOnCommit = 2

func TestLocalDataStore(t *testing.T) {
	suite.Run(t, &LocalDataStoreSuite{})
}

func TestRemoteDataStore(t *testing.T) {
	suite.Run(t, &RemoteDataStoreSuite{})
}

type DataStoreSuite struct {
	suite.Suite
	cs     *chunks.TestStore
	ds     DataStore
	makeDs func(chunks.ChunkStore) DataStore
}

type LocalDataStoreSuite struct {
	DataStoreSuite
}

func (suite *LocalDataStoreSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDs = NewDataStore
	suite.ds = suite.makeDs(suite.cs)
}

type RemoteDataStoreSuite struct {
	DataStoreSuite
}

func (suite *RemoteDataStoreSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.makeDs = func(cs chunks.ChunkStore) DataStore {
		hbs := newHTTPBatchStoreForTest(suite.cs)
		return &RemoteDataStoreClient{newDataStoreCommon(hbs, hbs)}
	}
	suite.ds = suite.makeDs(suite.cs)
}

func (suite *DataStoreSuite) TearDownTest() {
	suite.ds.Close()
	suite.cs.Close()
}

func (suite *DataStoreSuite) TestReadWriteCache() {
	var v types.Value = types.Bool(true)
	suite.NotEqual(ref.Ref{}, suite.ds.WriteValue(v))
	r := suite.ds.WriteValue(v).TargetRef()
	commit := NewCommit()
	newDs, err := suite.ds.Commit("foo", commit.Set(ValueField, v))
	suite.NoError(err)
	suite.Equal(1, suite.cs.Writes-writesOnCommit)

	v = newDs.ReadValue(r)
	suite.True(v.Equals(types.Bool(true)))
}

func (suite *DataStoreSuite) TestWriteRefToNonexistentValue() {
	suite.Panics(func() { suite.ds.WriteValue(types.NewRef(types.Bool(true).Ref())) })
}

func (suite *DataStoreSuite) TestWriteWrongTypeRef() {
	b := types.Bool(true)
	blob := types.NewEmptyBlob()
	suite.NotEqual(ref.Ref{}, suite.ds.WriteValue(b))

	suite.Panics(func() { suite.ds.WriteValue(types.NewTypedRef(blob.Type(), b.Ref())) })
}

func (suite *DataStoreSuite) TestWriteValueTypeRef() {
	b := types.Bool(true)
	suite.NotEqual(ref.Ref{}, suite.ds.WriteValue(b))

	suite.NotPanics(func() { suite.ds.WriteValue(types.NewRef(b.Ref())) })
}

func (suite *DataStoreSuite) TestReadValueTypeRefPanics_BUG1121() {
	b := types.NewEmptyBlob()
	suite.NotEqual(ref.Ref{}, suite.ds.WriteValue(b))

	datasetID := "ds1"
	aCommit := NewCommit().Set(ValueField, types.NewRef(b.Ref()))
	ds2, err := suite.ds.Commit(datasetID, aCommit)
	suite.NoError(err)

	_, ok := ds2.MaybeHead(datasetID)
	suite.True(ok)
	// Fix BUG 1121 and then uncomment this line and delete the one after
	// suite.NotPanics(func() { ds2.WriteValue(types.NewRefOfBlob(b.Ref())) })
	suite.Panics(func() { ds2.WriteValue(types.NewTypedRefFromValue(b)) })
}

func (suite *DataStoreSuite) TestTolerateUngettableRefs() {
	suite.Nil(suite.ds.ReadValue(ref.Ref{}))
}

func (suite *DataStoreSuite) TestDataStoreCommit() {
	datasetID := "ds1"
	datasets := suite.ds.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	ds2, err := suite.ds.Commit(datasetID, aCommit)
	suite.NoError(err)

	// The old datastore still has no head.
	_, ok := suite.ds.MaybeHead(datasetID)
	suite.False(ok)

	// The new datastore has |a|.
	aCommit1 := ds2.Head(datasetID)
	suite.True(aCommit1.Get(ValueField).Equals(a))
	suite.ds = ds2

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b|
	//   \----|c|
	// Should be disallowed.
	c := types.NewString("c")
	cCommit := NewCommit().Set(ValueField, c).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, cCommit)
	suite.Error(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))

	// |a| <- |b| <- |d|
	d := types.NewString("d")
	dCommit := NewCommit().Set(ValueField, d).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, dCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(d))

	// Attempt to recommit |b| with |a| as parent.
	// Should be disallowed.
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.Error(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(d))

	// Add a commit to a different datasetId
	_, err = suite.ds.Commit("otherDs", aCommit)
	suite.NoError(err)

	// Get a fresh datastore, and verify that both datasets are present
	newDs := suite.makeDs(suite.cs)
	datasets2 := newDs.Datasets()
	suite.Equal(uint64(2), datasets2.Len())
	newDs.Close()
}

func (suite *DataStoreSuite) TestDataStoreDelete() {
	datasetID1, datasetID2 := "ds1", "ds2"
	datasets := suite.ds.Datasets()
	suite.Zero(datasets.Len())

	// |a|
	var err error
	a := types.NewString("a")
	suite.ds, err = suite.ds.Commit(datasetID1, NewCommit().Set(ValueField, a))
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID1).Get(ValueField).Equals(a))

	// ds1; |a|, ds2: |b|
	b := types.NewString("b")
	suite.ds, err = suite.ds.Commit(datasetID2, NewCommit().Set(ValueField, b))
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID2).Get(ValueField).Equals(b))

	suite.ds, err = suite.ds.Delete(datasetID1)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID2).Get(ValueField).Equals(b))
	h, present := suite.ds.MaybeHead(datasetID1)
	suite.False(present, "Dataset %s should not be present, but head is %v", datasetID1, h.Get(ValueField))

	// Get a fresh datastore, and verify that only ds1 is present
	newDs := suite.makeDs(suite.cs)
	datasets = newDs.Datasets()
	suite.Equal(uint64(1), datasets.Len())
	_, present = suite.ds.MaybeHead(datasetID2)
	suite.True(present, "Dataset %s should be present", datasetID2)
	newDs.Close()
}

func (suite *DataStoreSuite) TestDataStoreDeleteConcurrent() {
	datasetID := "ds1"
	suite.Zero(suite.ds.Datasets().Len())
	var err error

	// |a|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	suite.ds, err = suite.ds.Commit(datasetID, aCommit)
	suite.NoError(err)

	// |a| <- |b|
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	ds2, err := suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(a))
	suite.True(ds2.Head(datasetID).Get(ValueField).Equals(b))

	suite.ds, err = suite.ds.Delete(datasetID)
	suite.NoError(err)
	h, present := suite.ds.MaybeHead(datasetID)
	suite.False(present, "Dataset %s should not be present, but head is %v", datasetID, h.Get(ValueField))
	h, present = ds2.MaybeHead(datasetID)
	suite.True(present, "Dataset %s should be present", datasetID)

	// Get a fresh datastore, and verify that no datastores are present
	newDs := suite.makeDs(suite.cs)
	suite.Equal(uint64(0), newDs.Datasets().Len())
	newDs.Close()
}

func (suite *DataStoreSuite) TestDataStoreConcurrency() {
	datasetID := "ds1"
	var err error

	// Setup:
	// |a| <- |b|
	a := types.NewString("a")
	aCommit := NewCommit().Set(ValueField, a)
	suite.ds, err = suite.ds.Commit(datasetID, aCommit)
	b := types.NewString("b")
	bCommit := NewCommit().Set(ValueField, b).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(aCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, bCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(b))

	// Important to create this here.
	ds2 := suite.makeDs(suite.cs)

	// Change 1:
	// |a| <- |b| <- |c|
	c := types.NewString("c")
	cCommit := NewCommit().Set(ValueField, c).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	suite.ds, err = suite.ds.Commit(datasetID, cCommit)
	suite.NoError(err)
	suite.True(suite.ds.Head(datasetID).Get(ValueField).Equals(c))

	// Change 2:
	// |a| <- |b| <- |e|
	// Should be disallowed, DataStore returned by Commit() should have |c| as Head.
	e := types.NewString("e")
	eCommit := NewCommit().Set(ValueField, e).Set(ParentsField, NewSetOfRefOfCommit().Insert(types.NewTypedRefFromValue(bCommit)))
	ds2, err = ds2.Commit(datasetID, eCommit)
	suite.Error(err)
	suite.True(ds2.Head(datasetID).Get(ValueField).Equals(c))
}
