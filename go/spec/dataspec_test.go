// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

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
	assert.Equal(s1.String(), store.ReadValue(s1.Hash()).(types.String).String())
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
	assert.Equal(types.Bool(true), store.ReadValue(r.TargetHash()))
}

func TestMemDataset(t *testing.T) {
	assert := assert.New(t)

	spec := "mem::datasetTest"
	sp1, err := ParseDatasetSpec(spec)
	assert.NoError(err)
	dataset1, err := sp1.Dataset()
	assert.NoError(err)
	commit := types.NewString("Commit Value")
	dsTest, err := dataset1.Commit(commit)
	assert.NoError(err)
	assert.EqualValues(commit, dsTest.HeadValue())
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

	spec := fmt.Sprintf("ldb:%s::%s", ldbPath, id)
	sp, err := ParseDatasetSpec(spec)
	assert.NoError(err)
	dataset, err := sp.Dataset()
	assert.NoError(err)
	assert.EqualValues(commit, dataset.HeadValue())

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

	spec2 := fmt.Sprintf("ldb:%s::%s", ldbpath, dsId)
	assert.NoError(err)
	sp1, err := ParseDatasetSpec(spec2)
	assert.NoError(err)
	dataset2, err := sp1.Dataset()
	assert.NoError(err)
	r2 := dataset2.HeadValue()
	s2 := r2.(types.Ref).TargetValue(dataset2.Database())
	assert.Equal(s1.String(), s2.(types.String).String())
	dataset2.Database().Close()

	spec3 := fmt.Sprintf("ldb:%s::%s", ldbpath, s1.Hash().String())
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
	r1 := dataset1.Head().Hash()
	dataset1.Database().Close()

	spec2 := fmt.Sprintf("ldb:%s::%s", ldbPath, r1.String())
	sp2, err := ParsePathSpec(spec2)
	assert.NoError(err)
	database, v2, err := sp2.Value()
	assert.NoError(err)

	assert.EqualValues(r1.String(), v2.Hash().String())
	database.Close()
}

func TestDatabaseSpecs(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{"mem:stuff", "mem:", "http:", "https:", "random:", "random:random", "http://some.com/wi::rd/path", "/file/ba:d"}
	for _, spec := range badSpecs {
		_, err := ParseDatabaseSpec(spec)
		assert.Error(err)
	}

	testCases := []map[string]string{
		map[string]string{"spec": "http://localhost:8000", "scheme": "http", "path": "//localhost:8000"},
		map[string]string{"spec": "http://localhost:8000/fff", "scheme": "http", "path": "//localhost:8000/fff"},
		map[string]string{"spec": "https://local.attic.io/john/doe", "scheme": "https", "path": "//local.attic.io/john/doe"},
		map[string]string{"spec": "ldb:/filesys/john/doe", "scheme": "ldb", "path": "/filesys/john/doe"},
		map[string]string{"spec": "./john/doe", "scheme": "ldb", "path": "./john/doe"},
		map[string]string{"spec": "john/doe", "scheme": "ldb", "path": "john/doe"},
		map[string]string{"spec": "/john/doe", "scheme": "ldb", "path": "/john/doe"},
		map[string]string{"spec": "mem", "scheme": "mem", "path": ""},
		map[string]string{"spec": "http://server.com/john/doe?access_token=jane", "scheme": "http", "path": "//server.com/john/doe?access_token=jane", "accessToken": "jane"},
		map[string]string{"spec": "https://server.com/john/doe/?arg=2&qp1=true&access_token=jane", "scheme": "https", "path": "//server.com/john/doe/?arg=2&qp1=true&access_token=jane", "accessToken": "jane"},
	}

	for _, tc := range testCases {
		dbSpec, err := ParseDatabaseSpec(tc["spec"])
		assert.NoError(err)
		assert.Equal(DatabaseSpec{Protocol: tc["scheme"], Path: tc["path"], accessToken: tc["accessToken"]}, dbSpec)
	}
}

func TestDatasetSpecs(t *testing.T) {
	assert := assert.New(t)
	badSpecs := []string{"mem", "mem:", "mem:::ds", "http", "http:", "http://foo", "monkey", "monkey:balls", "http::dsname", "mem:/a/bogus/path:dsname", "http://localhost:8000/one"}

	for _, spec := range badSpecs {
		_, err := ParseDatasetSpec(spec)
		assert.Error(err)
	}

	invalidDatasetNames := []string{" ", "", "$", "#", ":", "\n", "💩"}
	for _, s := range invalidDatasetNames {
		_, err := ParseDatasetSpec("mem::" + s)
		assert.Error(err)
	}

	validDatasetNames := []string{"a", "Z", "0", "/", "-", "_"}
	for _, s := range validDatasetNames {
		_, err := ParseDatasetSpec("mem::" + s)
		assert.NoError(err)
	}

	testCases := []map[string]string{
		map[string]string{"spec": "http://localhost:8000::ds1", "scheme": "http", "path": "//localhost:8000", "ds": "ds1"},
		map[string]string{"spec": "http://localhost:8000/john/doe/::ds2", "scheme": "http", "path": "//localhost:8000/john/doe/", "ds": "ds2"},
		map[string]string{"spec": "https://local.attic.io/john/doe::ds3", "scheme": "https", "path": "//local.attic.io/john/doe", "ds": "ds3"},
		map[string]string{"spec": "http://local.attic.io/john/doe::ds1", "scheme": "http", "path": "//local.attic.io/john/doe", "ds": "ds1"},
		map[string]string{"spec": "ldb:/filesys/john/doe::ds/one", "scheme": "ldb", "path": "/filesys/john/doe", "ds": "ds/one"},
		map[string]string{"spec": "http://localhost:8000/john/doe?access_token=abc::ds/one", "scheme": "http", "path": "//localhost:8000/john/doe?access_token=abc", "accessToken": "abc", "ds": "ds/one"},
		map[string]string{"spec": "https://localhost:8000?qp1=x&access_token=abc&qp2=y::ds/one", "scheme": "https", "path": "//localhost:8000?qp1=x&access_token=abc&qp2=y", "accessToken": "abc", "ds": "ds/one"},
	}

	for _, tc := range testCases {
		dsSpec, err := ParseDatasetSpec(tc["spec"])
		assert.NoError(err)
		dbSpec1 := DatabaseSpec{Protocol: tc["scheme"], Path: tc["path"], accessToken: tc["accessToken"]}
		assert.Equal(DatasetSpec{DbSpec: dbSpec1, DatasetName: tc["ds"]}, dsSpec)
	}
}

func TestRefSpec(t *testing.T) {
	assert := assert.New(t)

	testCases := []map[string]string{
		map[string]string{"spec": "http://local.attic.io/john/doe::sha1-0123456789012345678901234567890123456789", "scheme": "http", "path": "//local.attic.io/john/doe", "ref": "sha1-0123456789012345678901234567890123456789"},
		map[string]string{"spec": "ldb:/filesys/john/doe::sha1-0123456789012345678901234567890123456789", "scheme": "ldb", "path": "/filesys/john/doe", "ref": "sha1-0123456789012345678901234567890123456789"},
		map[string]string{"spec": "mem::sha1-0123456789012345678901234567890123456789", "scheme": "mem", "ref": "sha1-0123456789012345678901234567890123456789"},
	}

	for _, tc := range testCases {
		refSpec, err := ParseRefSpec(tc["spec"])
		assert.NoError(err)
		dbSpec1 := DatabaseSpec{Protocol: tc["scheme"], Path: tc["path"], accessToken: tc["accessToken"]}
		assert.Equal(RefSpec{DbSpec: dbSpec1, Ref: hash.Parse(tc["ref"])}, refSpec)
	}
}

func TestPathSpec(t *testing.T) {
	assert := assert.New(t)

	testCases := []map[string]string{
		map[string]string{"spec": "http://local.attic.io/john/doe::sha1-0123456789012345678901234567890123456789", "scheme": "http", "path": "//local.attic.io/john/doe", "ref": "sha1-0123456789012345678901234567890123456789"},
		map[string]string{"spec": "http://localhost:8000/john/doe/::ds1", "scheme": "http", "path": "//localhost:8000/john/doe/", "ds": "ds1"},
	}

	for _, tc := range testCases {
		pathSpec, err := ParsePathSpec(tc["spec"])
		assert.NoError(err)
		dbSpec1 := DatabaseSpec{Protocol: tc["scheme"], Path: tc["path"], accessToken: tc["accessToken"]}
		if tc["ref"] != "" {
			assert.Equal(&RefSpec{DbSpec: dbSpec1, Ref: hash.Parse(tc["ref"])}, pathSpec.(*RefSpec))
		} else {
			assert.Equal(&DatasetSpec{DbSpec: dbSpec1, DatasetName: tc["ds"]}, pathSpec.(*DatasetSpec))
		}
	}

	_, err := ParsePathSpec("http://local.attic.io")
	assert.Error(err)

}
