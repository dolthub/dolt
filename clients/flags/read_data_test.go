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

func TestParseDataStoreFromHTTP(t *testing.T) {
	if os.Getenv("RUN_HTTP_FLAGS_TESTS") == "" {
		t.Skip("Skipping flaky TestParseDataStoreFromHTTP; to enable set RUN_HTTP_FLAGS_TESTS env var")
	}

	assert := assert.New(t)

	server := datas.NewRemoteDataStoreServer(chunks.NewTestStoreFactory(), 8000)
	go server.Run()

	ds := datas.NewRemoteDataStore("http://localhost:8000", "")

	r := ds.WriteValue(types.Bool(true))

	ds.Close()

	datastoreName := "http://localhost:8000"
	dsTest, err := ParseDataStore(datastoreName)

	assert.NoError(err)
	assert.Equal(types.Bool(true), dsTest.ReadValue(r.TargetRef()))

	dsTest.Close()
	server.Stop()
}

func TestParseDataStoreFromLDB(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	cs := chunks.NewLevelDBStore(filepath.Join(dir, "name"), "", 24, false)
	ds := datas.NewDataStore(cs)

	r := ds.WriteValue(types.Bool(true))

	ds.Close()

	datastoreName := "ldb:" + filepath.Join(dir, "name")
	dsTest, errRead := ParseDataStore(datastoreName)

	assert.NoError(errRead)
	assert.Equal(types.Bool(true), dsTest.ReadValue(r.TargetRef()))

	dsTest.Close()
}

func TestParseDataStoreFromMem(t *testing.T) {
	assert := assert.New(t)

	datastoreName := "mem:"
	dsTest, err := ParseDataStore(datastoreName)

	r := dsTest.WriteValue(types.Bool(true))

	assert.NoError(err)
	assert.Equal(types.Bool(true), dsTest.ReadValue(r.TargetRef()))
}

func TestDataStoreBadInput(t *testing.T) {
	assert := assert.New(t)

	badName1 := "mem"
	ds, err := ParseDataStore(badName1)

	assert.Error(err)
	assert.Nil(ds)
}

func TestParseDatasetFromHTTP(t *testing.T) {
	if os.Getenv("RUN_HTTP_FLAGS_TESTS") == "" {
		t.Skip("Skipping flaky TestParseDatasetFromHTTP; to enable set RUN_HTTP_FLAGS_TESTS env var")
	}
	assert := assert.New(t)
	server := datas.NewRemoteDataStoreServer(chunks.NewTestStoreFactory(), 8001)
	go server.Run()

	ds := datas.NewRemoteDataStore("http://localhost:8001", "")
	id := "datasetTest"

	set := dataset.NewDataset(ds, id)
	commit := types.NewString("Commit Value")
	set, err := set.Commit(commit)
	assert.NoError(err)

	ds.Close()

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
	ds := datas.NewDataStore(cs)
	id := "testdataset"

	set := dataset.NewDataset(ds, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	ds.Close()

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
	ds := datas.NewDataStore(cs)
	id := "testdataset"

	set := dataset.NewDataset(ds, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	ds.Close()

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
	ds := datas.NewDataStore(cs)
	id := "testdataset"

	set := dataset.NewDataset(ds, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)

	ref := set.Head().Ref()

	ds.Close()

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
	ds := datas.NewDataStore(cs)

	r := ds.WriteValue(types.Bool(true))

	ds.Close()

	dsTest, errRead := ParseDataStore("")

	assert.NoError(errRead)
	assert.Equal(types.Bool(true), dsTest.ReadValue(r.TargetRef()))

	dsTest.Close()
}
