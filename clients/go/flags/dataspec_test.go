package flags

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

// TODO: implement this with mock httpService
func disabledTestHTTPDatabase(t *testing.T) {
	assert := assert.New(t)
	const port = 8017
	const testString = "A String for testing"
	const dsetId = "testds"
	spec := fmt.Sprintf("http://localhost:%d/", port)

	server := datas.NewRemoteDatabaseServer(chunks.NewTestStore(), port)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Run()
		wg.Done()
	}()
	time.Sleep(time.Second)

	sp1, err := ParseDatabaseSpec(spec)
	assert.NoError(err)
	store1, err := sp1.Database()
	assert.NoError(err)
	r1 := store1.WriteValue(types.NewString(testString))
	store1, err = store1.Commit(dsetId, datas.NewCommit().Set(datas.ValueField, r1))
	assert.NoError(err)
	store1.Close()

	sp2, err := ParseDatabaseSpec(spec)
	assert.NoError(err)
	store2, err := sp2.Database()
	assert.NoError(err)
	assert.Equal(types.NewString(testString), store2.ReadValue(r1.TargetRef()))

	server.Stop()
	wg.Wait()
}

func TestLDBDatabase(t *testing.T) {
	assert := assert.New(t)

	d1 := os.TempDir()
	dir, err := ioutil.TempDir(d1, "flags")
	assert.NoError(err)
	ldbDir := path.Join(dir, "store")
	spec := fmt.Sprintf("ldb:%s", path.Join(dir, "store"))

	cs := chunks.NewLevelDBStoreUseFlags(ldbDir, "")
	ds := datas.NewDatabase(cs)

	s1 := types.NewString("A String")
	s1Ref := ds.WriteValue(s1)
	ds.Commit("testDs", datas.NewCommit().Set(datas.ValueField, s1Ref))
	ds.Close()

	sp, errRead := ParseDatabaseSpec(spec)
	assert.NoError(errRead)
	store, err := sp.Database()
	assert.NoError(err)
	assert.Equal(s1.String(), store.ReadValue(s1.Ref()).(types.String).String())
	store.Close()
	os.Remove(dir)
}

func TestMemDatabase(t *testing.T) {
	assert := assert.New(t)

	spec := "mem"
	sp, err := ParseDatabaseSpec(spec)
	assert.NoError(err)
	store, err := sp.Database()
	assert.NoError(err)
	r := store.WriteValue(types.Bool(true))

	assert.NoError(err)
	assert.Equal(types.Bool(true), store.ReadValue(r.TargetRef()))
}

// TODO: implement this with mock httpService
func disabledTestHTTPDataset(t *testing.T) {
	assert := assert.New(t)
	const port = 8018
	const datasetId = "dsTest"
	spec := fmt.Sprintf("http://localhost:%d", port)
	datasetSpec := fmt.Sprintf("%s:%s", spec, datasetId)

	server := datas.NewRemoteDatabaseServer(chunks.NewTestStore(), port)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Run()
		wg.Done()
	}()
	time.Sleep(time.Second)

	sp1, err := ParseDatabaseSpec(spec)
	assert.NoError(err)
	store, err := sp1.Database()
	assert.NoError(err)

	dataset1 := dataset.NewDataset(store, datasetId)
	s1 := types.NewString("Commit Value")
	dataset1, err = dataset1.Commit(s1)
	assert.NoError(err)
	store.Close()

	sp2, err := ParseDatasetSpec(datasetSpec)
	assert.NoError(err)
	dataset2, err := sp2.Dataset()
	assert.NoError(err)
	assert.EqualValues(s1, dataset2.Head().Get(datas.ValueField))

	server.Stop()
	wg.Wait()
}

func TestMemDataset(t *testing.T) {
	assert := assert.New(t)

	spec := "mem:datasetTest"
	sp1, err := ParseDatasetSpec(spec)
	assert.NoError(err)
	dataset1, err := sp1.Dataset()
	assert.NoError(err)
	commit := types.NewString("Commit Value")
	dsTest, err := dataset1.Commit(commit)
	assert.NoError(err)
	assert.EqualValues(commit, dsTest.Head().Get(datas.ValueField))
}

func TestLDBDataset(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	ldbPath := path.Join(dir, "name")
	cs := chunks.NewLevelDBStoreUseFlags(ldbPath, "")
	ds := datas.NewDatabase(cs)
	id := "dsName"

	set := dataset.NewDataset(ds, id)
	commit := types.NewString("Commit Value")
	set, err = set.Commit(commit)
	assert.NoError(err)
	ds.Close()

	spec := fmt.Sprintf("ldb:%s:%s", ldbPath, id)
	sp, err := ParseDatasetSpec(spec)
	assert.NoError(err)
	dataset, err := sp.Dataset()
	assert.NoError(err)
	assert.EqualValues(commit, dataset.Head().Get(datas.ValueField))

	os.Remove(dir)
}

func TestLDBObject(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	ldbpath := path.Join(dir, "xx-yy")
	dsId := "dsId"

	cs1 := chunks.NewLevelDBStoreUseFlags(ldbpath, "")
	store1 := datas.NewDatabase(cs1)
	dataset1 := dataset.NewDataset(store1, dsId)
	s1 := types.NewString("Commit Value")
	r1 := store1.WriteValue(s1)
	_, err = dataset1.Commit(r1)
	assert.NoError(err)
	store1.Close()

	spec2 := fmt.Sprintf("ldb:%s:%s", ldbpath, dsId)
	assert.NoError(err)
	sp1, err := ParseDatasetSpec(spec2)
	assert.NoError(err)
	dataset2, err := sp1.Dataset()
	assert.NoError(err)
	r2 := dataset2.Head().Get(datas.ValueField)
	s2 := r2.(types.Ref).TargetValue(dataset2.Store())
	assert.Equal(s1.String(), s2.(types.String).String())
	dataset2.Store().Close()

	spec3 := fmt.Sprintf("ldb:%s:%s", ldbpath, s1.Ref().String())
	sp3, err := ParsePathSpec(spec3)
	database, v3, err := sp3.Value()
	assert.Equal(s1.String(), v3.(types.String).String())
	database.Close()
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	datasetId := "dsName"

	ldbPath := path.Join(dir, "/name")
	cs1 := chunks.NewLevelDBStoreUseFlags(ldbPath, "")
	database1 := datas.NewDatabase(cs1)
	dataset1 := dataset.NewDataset(database1, datasetId)
	commit := types.NewString("Commit Value")
	dataset1, err = dataset1.Commit(commit)
	assert.NoError(err)
	r1 := dataset1.Head().Ref()
	dataset1.Store().Close()

	spec2 := fmt.Sprintf("ldb:%s:%s", ldbPath, r1.String())
	sp2, err := ParsePathSpec(spec2)
	assert.NoError(err)
	database, v2, err := sp2.Value()
	assert.NoError(err)

	assert.EqualValues(r1.String(), v2.Ref().String())
	database.Close()
}

func TestDatabaseSpecs(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{"mem:", "mem:stuff", "http:", "https:", "random:", "random:random"}
	for _, spec := range badSpecs {
		_, err := ParseDatabaseSpec(spec)
		assert.Error(err)
	}

	storeSpec, err := ParseDatabaseSpec("http://localhost:8000")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "http", Path: "//localhost:8000"}, storeSpec)

	storeSpec, err = ParseDatabaseSpec("http://localhost:8000/")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "http", Path: "//localhost:8000"}, storeSpec)

	storeSpec, err = ParseDatabaseSpec("http://localhost:8000/fff")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "http", Path: "//localhost:8000/fff"}, storeSpec)

	storeSpec, err = ParseDatabaseSpec("https://local.attic.io/john/doe")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "https", Path: "//local.attic.io/john/doe"}, storeSpec)

	storeSpec, err = ParseDatabaseSpec("ldb:/filesys/john/doe")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "ldb", Path: "/filesys/john/doe"}, storeSpec)

	storeSpec, err = ParseDatabaseSpec("mem")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "mem"}, storeSpec)
}

func TestDatasetSpecs(t *testing.T) {
	assert := assert.New(t)
	badSpecs := []string{"mem", "mem:", "http", "http:", "http://foo", "monkey", "monkey:balls", "http::dsname", "mem:/a/bogus/path:dsname"}

	for _, spec := range badSpecs {
		_, err := ParseDatasetSpec(spec)
		assert.Error(err)
	}

	setSpec, err := ParseDatasetSpec("http://localhost:8000:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//localhost:8000"}, DatasetName: "dsname"}, setSpec)

	setSpec, err = ParseDatasetSpec("http://localhost:8000/john/doe/:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//localhost:8000/john/doe"}, DatasetName: "dsname"}, setSpec)

	setSpec, err = ParseDatasetSpec("https://local.attic.io/john/doe:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "https", Path: "//local.attic.io/john/doe"}, DatasetName: "dsname"}, setSpec)

	setSpec, err = ParseDatasetSpec("http://local.attic.io/john/doe:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//local.attic.io/john/doe"}, DatasetName: "dsname"}, setSpec)

	setSpec, err = ParseDatasetSpec("ldb:/filesys/john/doe:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/filesys/john/doe"}, DatasetName: "dsname"}, setSpec)

	setSpec, err = ParseDatasetSpec("mem:dsname")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "mem"}, DatasetName: "dsname"}, setSpec)
}

func TestRefSpec(t *testing.T) {
	assert := assert.New(t)

	testRef := ref.Parse("sha1-0123456789012345678901234567890123456789")

	refSpec, err := ParseRefSpec("http://local.attic.io/john/doe:sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//local.attic.io/john/doe"}, Ref: testRef}, refSpec)

	refSpec, err = ParseRefSpec("ldb:/filesys/john/doe:sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/filesys/john/doe"}, Ref: testRef}, refSpec)

	refSpec, err = ParseRefSpec("mem:sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "mem"}, Ref: testRef}, refSpec)
}

func TestPathSpec(t *testing.T) {
	assert := assert.New(t)

	testRef := ref.Parse("sha1-0123456789012345678901234567890123456789")

	pathSpec, err := ParsePathSpec("http://local.attic.io/john/doe:sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	refSpec := pathSpec.(*RefSpec)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//local.attic.io/john/doe"}, Ref: testRef}, *refSpec)

	pathSpec, err = ParsePathSpec("http://localhost:8000/john/doe/:dsname")
	assert.NoError(err)
	setSpec := pathSpec.(*DatasetSpec)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "http", Path: "//localhost:8000/john/doe"}, DatasetName: "dsname"}, *setSpec)

	_, err = ParsePathSpec("http://local.attic.io")
	assert.Error(err)
}

// Todo: implemeent ldb defaults...
func disabledTestRefSpec(t *testing.T) {
	assert := assert.New(t)

	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", "/u/testuser")
	defer os.Setenv("HOME", oldHome)

	setSpec, err := ParseDatasetSpec("/filesys/john/doe:dsName")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/filesys/john/doe"}, DatasetName: "dsName"}, setSpec)

	setSpec, err = ParseDatasetSpec("xyz")
	assert.NoError(err)
	assert.Equal(DatasetSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/u/testuser/.noms"}, DatasetName: "xyz"}, setSpec)

	storeSpec, err := ParseDatasetSpec("/path/to/somewhere")
	assert.NoError(err)
	assert.Equal(DatabaseSpec{Protocol: "ldb", Path: "/path/to/somewhere"}, storeSpec)

	testRef := ref.Parse("sha1-0123456789012345678901234567890123456789")

	refSpec, err := ParseRefSpec("/filesys/john/doe:sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/filesys/john/doe"}, Ref: testRef}, refSpec)

	refSpec, err = ParseRefSpec("sha1-0123456789012345678901234567890123456789")
	assert.NoError(err)
	assert.Equal(RefSpec{StoreSpec: DatabaseSpec{Protocol: "ldb", Path: "/u/testuser/.noms"}, Ref: testRef}, refSpec)
}
