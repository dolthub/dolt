// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func TestMemDatabaseSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForDatabase("mem")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.True(spec.Path.IsEmpty())

	s := types.String("hello")
	db := spec.GetDatabase(context.Background())
	db.WriteValue(context.Background(), s)
	assert.Equal(s, db.ReadValue(context.Background(), s.Hash()))
}

func TestMemDatasetSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForDataset("mem::test")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.Equal("test", spec.Path.Dataset)
	assert.True(spec.Path.Path.IsEmpty())

	ds := spec.GetDataset(context.Background())
	_, ok := spec.GetDataset(context.Background()).MaybeHeadValue()
	assert.False(ok)

	s := types.String("hello")
	ds, err = spec.GetDatabase(context.Background()).CommitValue(context.Background(), ds, s)
	assert.NoError(err)
	assert.Equal(s, ds.HeadValue())
}

func TestMemHashPathSpec(t *testing.T) {
	assert := assert.New(t)

	s := types.String("hello")

	spec, err := ForPath("mem::#" + s.Hash().String())
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.False(spec.Path.IsEmpty())

	// This is a reasonable check but it causes the next GetValue to return nil:
	// assert.Nil(spec.GetValue())

	spec.GetDatabase(context.Background()).WriteValue(context.Background(), s)
	assert.Equal(s, spec.GetValue(context.Background()))
}

func TestMemDatasetPathSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForPath("mem::test.value[0]")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.False(spec.Path.IsEmpty())

	assert.Nil(spec.GetValue(context.Background()))

	db := spec.GetDatabase(context.Background())
	ds := db.GetDataset(context.Background(), "test")
	_, err = db.CommitValue(context.Background(), ds, types.NewList(context.Background(), db, types.Float(42)))
	assert.NoError(err)

	assert.Equal(types.Float(42), spec.GetValue(context.Background()))
}

func TestNBSDatabaseSpec(t *testing.T) {
	assert := assert.New(t)

	run := func(prefix string) {
		tmpDir, err := ioutil.TempDir("", "spec_test")
		assert.NoError(err)
		defer os.RemoveAll(tmpDir)

		s := types.String("string")

		// Existing database in the database are read from the spec.
		store1 := path.Join(tmpDir, "store1")
		os.Mkdir(store1, 0777)
		func() {
			db := datas.NewDatabase(nbs.NewLocalStore(context.Background(), store1, 8*(1<<20)))
			defer db.Close()
			r := db.WriteValue(context.Background(), s)
			_, err = db.CommitValue(context.Background(), db.GetDataset(context.Background(), "datasetID"), r)
			assert.NoError(err)
		}()

		spec1, err := ForDatabase(prefix + store1)
		assert.NoError(err)
		defer spec1.Close()

		assert.Equal("nbs", spec1.Protocol)
		assert.Equal(store1, spec1.DatabaseName)

		assert.Equal(s, spec1.GetDatabase(context.Background()).ReadValue(context.Background(), s.Hash()))

		// New databases can be created and read/written from.
		store2 := path.Join(tmpDir, "store2")
		os.Mkdir(store2, 0777)
		spec2, err := ForDatabase(prefix + store2)
		assert.NoError(err)
		defer spec2.Close()

		assert.Equal("nbs", spec2.Protocol)
		assert.Equal(store2, spec2.DatabaseName)

		db := spec2.GetDatabase(context.Background())
		db.WriteValue(context.Background(), s)
		r := db.WriteValue(context.Background(), s)
		_, err = db.CommitValue(context.Background(), db.GetDataset(context.Background(), "datasetID"), r)
		assert.NoError(err)
		assert.Equal(s, db.ReadValue(context.Background(), s.Hash()))
	}

	run("")
	run("nbs:")
}

// Skip LDB dataset and path tests: the database behaviour is tested in
// TestLDBDatabaseSpec, TestMemDatasetSpec/TestMem*PathSpec cover general
// dataset/path behaviour, and ForDataset/ForPath test LDB parsing.

func TestCloseSpecWithoutOpen(t *testing.T) {
	s, err := ForDatabase("mem")
	assert.NoError(t, err)
	s.Close()
}

func TestHref(t *testing.T) {
	assert := assert.New(t)

	sp, _ := ForDatabase("http://localhost")
	assert.Equal("http://localhost", sp.Href())
	sp, _ = ForDatabase("http://localhost/foo/bar/baz")
	assert.Equal("http://localhost/foo/bar/baz", sp.Href())
	sp, _ = ForDatabase("https://my.example.com/foo/bar/baz")
	assert.Equal("https://my.example.com/foo/bar/baz", sp.Href())
	sp, _ = ForDataset("https://my.example.com/foo/bar/baz::myds")
	assert.Equal("https://my.example.com/foo/bar/baz", sp.Href())
	sp, _ = ForDataset("https://my.example.com:8080/foo/bar/baz::myds")
	assert.Equal("https://my.example.com:8080/foo/bar/baz", sp.Href())
	sp, _ = ForPath("https://my.example.com/foo/bar/baz::myds.my.path")
	assert.Equal("https://my.example.com/foo/bar/baz", sp.Href())

	sp, _ = ForDatabase("aws://table/foo/bar/baz")
	assert.Equal("aws://table/foo/bar/baz", sp.Href())
	sp, _ = ForDataset("aws://table:bucket/foo/bar/baz::myds")
	assert.Equal("aws://table:bucket/foo/bar/baz", sp.Href())
	sp, _ = ForPath("aws://table:bucket/foo/bar/baz::myds.my.path")
	assert.Equal("aws://table:bucket/foo/bar/baz", sp.Href())

	sp, err := ForPath("mem::myds.my.path")
	assert.NoError(err)
	assert.Equal("", sp.Href())
}

func TestForDatabase(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{
		"mem:stuff",
		"mem::",
		"mem:",
		"http:",
		"http://",
		"http://%",
		"https:",
		"https://",
		"https://%",
		"ldb:",
		"random:",
		"random:random",
		"/file/ba:d",
		"aws://t:b",
		"aws://t",
		"aws://t:",
	}

	for _, spec := range badSpecs {
		_, err := ForDatabase(spec)
		assert.Error(err, spec)
	}

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, canonicalSpecIfAny string
	}{
		{"http://localhost:8000", "http", "//localhost:8000", ""},
		{"http://localhost:8000/fff", "http", "//localhost:8000/fff", ""},
		{"https://local.attic.io/john/doe", "https", "//local.attic.io/john/doe", ""},
		{"mem", "mem", "", ""},
		{tmpDir, "nbs", tmpDir, "nbs:" + tmpDir},
		{"nbs:" + tmpDir, "nbs", tmpDir, ""},
		{"http://server.com/john/doe?access_token=jane", "http", "//server.com/john/doe?access_token=jane", ""},
		{"https://server.com/john/doe/?arg=2&qp1=true&access_token=jane", "https", "//server.com/john/doe/?arg=2&qp1=true&access_token=jane", ""},
		{"http://some/::/one", "http", "//some/::/one", ""},
		{"http://::1", "http", "//::1", ""},
		{"http://192.30.252.154", "http", "//192.30.252.154", ""},
		{"http://::192.30.252.154", "http", "//::192.30.252.154", ""},
		{"http://0:0:0:0:0:ffff:c01e:fc9a", "http", "//0:0:0:0:0:ffff:c01e:fc9a", ""},
		{"http://::ffff:c01e:fc9a", "http", "//::ffff:c01e:fc9a", ""},
		{"http://::ffff::1e::9a", "http", "//::ffff::1e::9a", ""},
		{"aws://table:bucket/db", "aws", "//table:bucket/db", ""},
		{"aws://table/db", "aws", "//table/db", ""},
	}

	for _, tc := range testCases {
		spec, err := ForDatabase(tc.spec)
		assert.NoError(err, tc.spec)
		defer spec.Close()

		assert.Equal(tc.protocol, spec.Protocol)
		assert.Equal(tc.databaseName, spec.DatabaseName)
		assert.True(spec.Path.IsEmpty())

		if tc.canonicalSpecIfAny == "" {
			assert.Equal(tc.spec, spec.String())
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String())
		}
	}
}

func TestForDataset(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{
		"mem",
		"mem:",
		"mem:::ds",
		"http",
		"http:",
		"http://foo",
		"monkey",
		"monkey:balls",
		"http:::dsname",
		"mem:/a/bogus/path:dsname",
		"http://localhost:8000/one",
		"nbs:",
		"nbs:hello",
		"aws://t:b/db",
		"mem::foo.value",
	}

	for _, spec := range badSpecs {
		_, err := ForDataset(spec)
		assert.Error(err, spec)
	}

	invalidDatasetNames := []string{" ", "", "$", "#", ":", "\n", "💩"}
	for _, s := range invalidDatasetNames {
		_, err := ForDataset("mem::" + s)
		assert.Error(err)
	}

	validDatasetNames := []string{"a", "Z", "0", "/", "-", "_"}
	for _, s := range validDatasetNames {
		_, err := ForDataset("mem::" + s)
		assert.NoError(err)
	}

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, datasetName, canonicalSpecIfAny string
	}{
		{"http://localhost:8000::ds1", "http", "//localhost:8000", "ds1", ""},
		{"http://localhost:8000/john/doe/::ds2", "http", "//localhost:8000/john/doe/", "ds2", ""},
		{"https://local.attic.io/john/doe::ds3", "https", "//local.attic.io/john/doe", "ds3", ""},
		{"http://local.attic.io/john/doe::ds1", "http", "//local.attic.io/john/doe", "ds1", ""},
		{"nbs:" + tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", ""},
		{tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", "nbs:" + tmpDir + "::ds/one"},
		{"http://localhost:8000/john/doe?access_token=abc::ds/one", "http", "//localhost:8000/john/doe?access_token=abc", "ds/one", ""},
		{"https://localhost:8000?qp1=x&access_token=abc&qp2=y::ds/one", "https", "//localhost:8000?qp1=x&access_token=abc&qp2=y", "ds/one", ""},
		{"http://192.30.252.154::foo", "http", "//192.30.252.154", "foo", ""},
		{"http://::1::foo", "http", "//::1", "foo", ""},
		{"http://::192.30.252.154::foo", "http", "//::192.30.252.154", "foo", ""},
		{"http://0:0:0:0:0:ffff:c01e:fc9a::foo", "http", "//0:0:0:0:0:ffff:c01e:fc9a", "foo", ""},
		{"http://::ffff:c01e:fc9a::foo", "http", "//::ffff:c01e:fc9a", "foo", ""},
		{"http://::ffff::1e::9a::foo", "http", "//::ffff::1e::9a", "foo", ""},
		{"aws://table:bucket/db::ds", "aws", "//table:bucket/db", "ds", ""},
		{"aws://table/db::ds", "aws", "//table/db", "ds", ""},
	}

	for _, tc := range testCases {
		spec, err := ForDataset(tc.spec)
		assert.NoError(err, tc.spec)
		defer spec.Close()

		assert.Equal(tc.protocol, spec.Protocol)
		assert.Equal(tc.databaseName, spec.DatabaseName)
		assert.Equal(tc.datasetName, spec.Path.Dataset)

		if tc.canonicalSpecIfAny == "" {
			assert.Equal(tc.spec, spec.String())
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String())
		}
	}
}

func TestForPath(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{
		"mem::#",
		"mem::#s",
		"mem::#foobarbaz",
		"mem::#wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww",
	}

	for _, bs := range badSpecs {
		_, err := ForPath(bs)
		assert.Error(err)
	}

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, pathString, canonicalSpecIfAny string
	}{
		{"http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv", "http", "//local.attic.io/john/doe", "#0123456789abcdefghijklmnopqrstuv", ""},
		{tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", "nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv"},
		{"nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", ""},
		{"mem::#0123456789abcdefghijklmnopqrstuv", "mem", "", "#0123456789abcdefghijklmnopqrstuv", ""},
		{"http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv", "http", "//local.attic.io/john/doe", "#0123456789abcdefghijklmnopqrstuv", ""},
		{"http://localhost:8000/john/doe/::ds1", "http", "//localhost:8000/john/doe/", "ds1", ""},
		{"http://192.30.252.154::foo.bar", "http", "//192.30.252.154", "foo.bar", ""},
		{"http://::1::foo.bar.baz", "http", "//::1", "foo.bar.baz", ""},
		{"http://::192.30.252.154::baz[42]", "http", "//::192.30.252.154", "baz[42]", ""},
		{"http://0:0:0:0:0:ffff:c01e:fc9a::foo[42].bar", "http", "//0:0:0:0:0:ffff:c01e:fc9a", "foo[42].bar", ""},
		{"http://::ffff:c01e:fc9a::foo.foo", "http", "//::ffff:c01e:fc9a", "foo.foo", ""},
		{"http://::ffff::1e::9a::hello[\"world\"]", "http", "//::ffff::1e::9a", "hello[\"world\"]", ""},
		{"aws://table:bucket/db::foo.foo", "aws", "//table:bucket/db", "foo.foo", ""},
		{"aws://table/db::foo.foo", "aws", "//table/db", "foo.foo", ""},
	}

	for _, tc := range testCases {
		spec, err := ForPath(tc.spec)
		assert.NoError(err)
		defer spec.Close()

		assert.Equal(tc.protocol, spec.Protocol)
		assert.Equal(tc.databaseName, spec.DatabaseName)
		assert.Equal(tc.pathString, spec.Path.String())

		if tc.canonicalSpecIfAny == "" {
			assert.Equal(tc.spec, spec.String())
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String())
		}
	}
}

func TestPinPathSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForPath("mem::foo.value")
	assert.NoError(err)
	defer unpinned.Close()

	db := unpinned.GetDatabase(context.Background())
	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(42))

	pinned, ok := unpinned.Pin(context.Background())
	assert.True(ok)
	defer pinned.Close()

	head := db.GetDataset(context.Background(), "foo").Head()

	assert.Equal(head.Hash(), pinned.Path.Hash)
	assert.Equal(fmt.Sprintf("mem::#%s.value", head.Hash().String()), pinned.String())
	assert.Equal(types.Float(42), pinned.GetValue(context.Background()))
	assert.Equal(types.Float(42), unpinned.GetValue(context.Background()))

	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(43))
	assert.Equal(types.Float(42), pinned.GetValue(context.Background()))
	assert.Equal(types.Float(43), unpinned.GetValue(context.Background()))
}

func TestPinDatasetSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForDataset("mem::foo")
	assert.NoError(err)
	defer unpinned.Close()

	db := unpinned.GetDatabase(context.Background())
	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(42))

	pinned, ok := unpinned.Pin(context.Background())
	assert.True(ok)
	defer pinned.Close()

	head := db.GetDataset(context.Background(), "foo").Head()

	commitValue := func(val types.Value) types.Value {
		return val.(types.Struct).Get(datas.ValueField)
	}

	assert.Equal(head.Hash(), pinned.Path.Hash)
	assert.Equal(fmt.Sprintf("mem::#%s", head.Hash().String()), pinned.String())
	assert.Equal(types.Float(42), commitValue(pinned.GetValue(context.Background())))
	assert.Equal(types.Float(42), unpinned.GetDataset(context.Background(), ).HeadValue())

	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(43))
	assert.Equal(types.Float(42), commitValue(pinned.GetValue(context.Background())))
	assert.Equal(types.Float(43), unpinned.GetDataset(context.Background()).HeadValue())
}

func TestAlreadyPinnedPathSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForPath("mem::#imgp9mp1h3b9nv0gna6mri53dlj9f4ql.value")
	assert.NoError(err)
	pinned, ok := unpinned.Pin(context.Background())
	assert.True(ok)
	assert.Equal(unpinned, pinned)
}

func TestMultipleSpecsSameNBS(t *testing.T) {
	assert := assert.New(t)

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	spec1, err1 := ForDatabase(tmpDir)
	spec2, err2 := ForDatabase(tmpDir)

	assert.NoError(err1)
	assert.NoError(err2)

	s := types.String("hello")
	db := spec1.GetDatabase(context.Background())
	r := db.WriteValue(context.Background(), s)
	_, err = db.CommitValue(context.Background(), db.GetDataset(context.Background(), "datasetID"), r)
	assert.NoError(err)
	assert.Equal(s, spec2.GetDatabase(context.Background()).ReadValue(context.Background(), s.Hash()))
}

func TestAcccessingInvalidSpec(t *testing.T) {
	assert := assert.New(t)

	test := func(spec string) {
		sp, err := ForDatabase(spec)
		assert.Error(err)
		assert.Equal("", sp.Href())
		assert.Panics(func() { sp.GetDatabase(context.Background()) })
		assert.Panics(func() { sp.GetDatabase(context.Background()) })
		assert.Panics(func() { sp.NewChunkStore(context.Background()) })
		assert.Panics(func() { sp.NewChunkStore(context.Background()) })
		assert.Panics(func() { sp.Close() })
		assert.Panics(func() { sp.Close() })
		// Spec was created with ForDatabase, so dataset/path related functions
		// should just fail not panic.
		_, ok := sp.Pin(context.Background())
		assert.False(ok)
		assert.Equal(datas.Dataset{}, sp.GetDataset(context.Background()))
		assert.Nil(sp.GetValue(context.Background()))
	}

	test("")
	test("invalid:spec")
	test("💩:spec")
	test("http:")
	test("http:💩:")
}

type testProtocol struct {
	name string
}

func (t *testProtocol) NewChunkStore(sp Spec) (chunks.ChunkStore, error) {
	t.name = sp.DatabaseName
	return chunks.NewMemoryStoreFactory().CreateStore(context.Background(), ""), nil
}
func (t *testProtocol) NewDatabase(sp Spec) (datas.Database, error) {
	t.name = sp.DatabaseName
	cs, err := t.NewChunkStore(sp)
	d.PanicIfError(err)
	return datas.NewDatabase(cs), nil
}

func TestExternalProtocol(t *testing.T) {
	assert := assert.New(t)
	tp := testProtocol{}
	ExternalProtocols["test"] = &tp

	sp, err := ForDataset("test:foo::bar")
	assert.NoError(err)
	assert.Equal("test", sp.Protocol)
	assert.Equal("foo", sp.DatabaseName)

	cs := sp.NewChunkStore(context.Background())
	assert.Equal("foo", tp.name)
	c := chunks.NewChunk([]byte("hi!"))
	cs.Put(context.Background(), c)
	assert.True(cs.Has(context.Background(), c.Hash()))

	tp.name = ""
	ds := sp.GetDataset(context.Background())
	assert.Equal("foo", tp.name)

	ds, err = ds.Database().CommitValue(context.Background(), ds, types.String("hi!"))
	d.PanicIfError(err)

	assert.True(types.String("hi!").Equals(ds.HeadValue()))
}
