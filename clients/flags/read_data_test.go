package flags

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestParseDatabaseFromHTTP(t *testing.T) {
	if os.Getenv("RUN_HTTP_FLAGS_TESTS") == "" {
		t.Skip("Skipping flaky TestParseDatabaseFromHTTP; to enable set RUN_HTTP_FLAGS_TESTS env var")
	}

	assert := assert.New(t)

	server := datas.NewRemoteDatabaseServer(chunks.NewTestStoreFactory(), 8000)
	go server.Run()

	db := datas.NewRemoteDatabase("http://localhost:8000", "")

	r := db.WriteValue(types.Bool(true))

	db.Close()

	datastoreName := "http://localhost:8000"
	dbTest, err := ParseDatabase(datastoreName)

	assert.NoError(err)
	assert.Equal(types.Bool(true), dbTest.ReadValue(r.TargetRef()))

	dbTest.Close()
	server.Stop()
}

func TestParseDatabaseFromLDB(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	cs := chunks.NewLevelDBStore(filepath.Join(dir, "name"), "", 24, false)
	db := datas.NewDatabase(cs)

	r := db.WriteValue(types.Bool(true))

	db.Close()

	databaseName := "ldb:" + filepath.Join(dir, "name")
	dbTest, errRead := ParseDatabase(databaseName)

	assert.NoError(errRead)
	assert.Equal(types.Bool(true), dbTest.ReadValue(r.TargetRef()))

	dbTest.Close()
}

func TestParseDatabaseFromMem(t *testing.T) {
	assert := assert.New(t)

	databaseName := "mem:"
	dbTest, err := ParseDatabase(databaseName)

	r := dbTest.WriteValue(types.Bool(true))

	assert.NoError(err)
	assert.Equal(types.Bool(true), dbTest.ReadValue(r.TargetRef()))
}

func TestDatabaseBadInput(t *testing.T) {
	assert := assert.New(t)

	badName1 := "mem"
	db, err := ParseDatabase(badName1)

	assert.Error(err)
	assert.Nil(db)
}

func TestParseDatasetFromHTTP(t *testing.T) {
	if os.Getenv("RUN_HTTP_FLAGS_TESTS") == "" {
		t.Skip("Skipping flaky TestParseDatasetFromHTTP; to enable set RUN_HTTP_FLAGS_TESTS env var")
	}
	assert := assert.New(t)
	server := datas.NewRemoteDatabaseServer(chunks.NewTestStoreFactory(), 8001)
	go server.Run()

	db := datas.NewRemoteDatabase("http://localhost:8001", "")
	id := "datasetTest"

	set := dataset.NewDataset(db, id)
	commit := types.NewString("Commit Value")
	set, err := set.Commit(commit)
	assert.NoError(err)

	db.Close()

	datasetName := "http://localhost:8001:datasetTest"
	setTest, err := ParseDataset(datasetName)

	assert.NoError(err)
	assert.EqualValues(commit, setTest.Head().Get(datas.ValueField))

	server.Stop()
}

func TestParseDatasetFromMem(t *testing.T) {
	assert := assert.New(t)

	datasetName := "mem::datasetTest"
	dsTest, errTest := ParseDataset(datasetName)

	assert.NoError(errTest)

	commit := types.NewString("Commit Value")
	dsTest, err := dsTest.Commit(commit)
	assert.NoError(err)

	assert.EqualValues(commit, dsTest.Head().Get(datas.ValueField))
}

func TestParseDatasetFromLDB(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	db := datas.NewDatabase(cs)
	id := "testdataset"

	set := dataset.NewDataset(db, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	db.Close()

	datasetName := "ldb:" + dir + "/name:" + id
	setTest, errRead := ParseDataset(datasetName)

	assert.NoError(errRead)
	assert.EqualValues(commit, setTest.Head().Get(datas.ValueField))
}

func TestDatasetBadInput(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	badName := "ldb:" + dir + "/name:--bad"
	ds, err := ParseDataset(badName)

	assert.Error(err)
	assert.NotNil(ds)

	badName2 := "ldb:" + dir
	ds, err = ParseDataset(badName2)

	assert.Error(err)
	assert.NotNil(ds)

	badName3 := "mem"
	ds, err = ParseDataset(badName3)

	assert.Error(err)
	assert.NotNil(ds)
}

func TestParseDatasetObjectFromLdb(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	db := datas.NewDatabase(cs)
	id := "testdataset"

	set := dataset.NewDataset(db, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	db.Close()

	datasetName := "ldb:" + dir + "/name:" + id
	setTest, ref, isDs, errRead := ParseObject(datasetName)

	assert.Zero(ref)
	assert.True(isDs)
	assert.NoError(errRead)
	assert.EqualValues(commit, setTest.Head().Get(datas.ValueField))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	db := datas.NewDatabase(cs)
	id := "testdataset"

	set := dataset.NewDataset(db, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	ref := set.Head().Ref()

	db.Close()

	objectName := "ldb:" + dir + "/name:" + ref.String()

	set, refTest, isDs, errRead := ParseObject(objectName)

	assert.EqualValues(ref.String(), refTest.String())
	assert.False(isDs)
	assert.NoError(errRead)
	assert.Zero(set)
}

func TestParseObjectBadInput(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)

	badName := "ldb:" + dir + "/name:sha2-78888888888"
	ds, ref, isDs, err := ParseObject(badName)

	//it interprets it as a dataset id

	assert.NoError(err)
	assert.NotNil(ds)
	assert.Zero(ref)
	assert.True(isDs)
}

func TestDefaultDatastore(t *testing.T) {
	assert := assert.New(t)

	home := os.Getenv("HOME")
	defer os.Setenv(home, "HOME")

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	os.Setenv(dir, "HOME")

	cs := chunks.NewLevelDBStore(filepath.Join(os.Getenv("HOME"), ".noms"), "", 24, false)
	db := datas.NewDatabase(cs)

	r := db.WriteValue(types.Bool(true))

	db.Close()

	dbTest, errRead := ParseDatabase("")

	assert.NoError(errRead)
	assert.Equal(types.Bool(true), dbTest.ReadValue(r.TargetRef()))

	dbTest.Close()
}
