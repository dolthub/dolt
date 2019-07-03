// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
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
	// TODO(binformat)
	assert.Equal(s, db.ReadValue(context.Background(), s.Hash(types.Format_7_18)))
}

func TestMemDatasetSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForDataset(types.Format_7_18, "mem::test")
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

	// TODO(binformat)
	spec, err := ForPath(types.Format_7_18, "mem::#" + s.Hash(types.Format_7_18).String())
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

	spec, err := ForPath(types.Format_7_18, "mem::test.value[0]")
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
		store1 := filepath.Join(tmpDir, "store1")
		os.Mkdir(store1, 0777)
		func() {
			cs, err := nbs.NewLocalStore(context.Background(), store1, 8*(1<<20))
			assert.NoError(err)
			db := datas.NewDatabase(cs)
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

		// TODO(binformat)
		assert.Equal(s, spec1.GetDatabase(context.Background()).ReadValue(context.Background(), s.Hash(types.Format_7_18)))

		// New databases can be created and read/written from.
		store2 := filepath.Join(tmpDir, "store2")
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
		// TODO(binformat)
		assert.Equal(s, db.ReadValue(context.Background(), s.Hash(types.Format_7_18)))
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

	sp, _ := ForDatabase("aws://table/foo/bar/baz")
	assert.Equal("aws://table/foo/bar/baz", sp.Href())
	sp, _ = ForDataset(types.Format_7_18, "aws://table:bucket/foo/bar/baz::myds")
	assert.Equal("aws://table:bucket/foo/bar/baz", sp.Href())
	sp, _ = ForPath(types.Format_7_18, "aws://table:bucket/foo/bar/baz::myds.my.path")
	assert.Equal("aws://table:bucket/foo/bar/baz", sp.Href())

	sp, err := ForPath(types.Format_7_18, "mem::myds.my.path")
	assert.NoError(err)
	assert.Equal("", sp.Href())
}

func TestForDatabase(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{
		"mem:stuff",
		"mem::",
		"mem:",
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
		{"mem", "mem", "", ""},
		{tmpDir, "nbs", tmpDir, "nbs:" + tmpDir},
		{"nbs:" + tmpDir, "nbs", tmpDir, ""},
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
			assert.Equal(tc.spec, spec.String(types.Format_7_18))
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String(types.Format_7_18))
		}
	}
}

func TestForDataset(t *testing.T) {
	assert := assert.New(t)

	badSpecs := []string{
		"mem",
		"mem:",
		"mem:::ds",
		"monkey",
		"monkey:balls",
		"mem:/a/bogus/path:dsname",
		"nbs:",
		"nbs:hello",
		"aws://t:b/db",
		"mem::foo.value",
	}

	for _, spec := range badSpecs {
		_, err := ForDataset(types.Format_7_18, spec)
		assert.Error(err, spec)
	}

	invalidDatasetNames := []string{" ", "", "$", "#", ":", "\n", "💩"}
	for _, s := range invalidDatasetNames {
		_, err := ForDataset(types.Format_7_18, "mem::" + s)
		assert.Error(err)
	}

	validDatasetNames := []string{"a", "Z", "0", "/", "-", "_"}
	for _, s := range validDatasetNames {
		_, err := ForDataset(types.Format_7_18, "mem::" + s)
		assert.NoError(err)
	}

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, datasetName, canonicalSpecIfAny string
	}{
		{"nbs:" + tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", ""},
		{tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", "nbs:" + tmpDir + "::ds/one"},
		{"aws://table:bucket/db::ds", "aws", "//table:bucket/db", "ds", ""},
		{"aws://table/db::ds", "aws", "//table/db", "ds", ""},
	}

	for _, tc := range testCases {
		spec, err := ForDataset(types.Format_7_18, tc.spec)
		assert.NoError(err, tc.spec)
		defer spec.Close()

		assert.Equal(tc.protocol, spec.Protocol)
		assert.Equal(tc.databaseName, spec.DatabaseName)
		assert.Equal(tc.datasetName, spec.Path.Dataset)

		if tc.canonicalSpecIfAny == "" {
			assert.Equal(tc.spec, spec.String(types.Format_7_18))
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String(types.Format_7_18))
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
		_, err := ForPath(types.Format_7_18, bs)
		assert.Error(err)
	}

	tmpDir, err := ioutil.TempDir("", "spec_test")
	assert.NoError(err)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, pathString, canonicalSpecIfAny string
	}{
		{tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", "nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv"},
		{"nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", ""},
		{"mem::#0123456789abcdefghijklmnopqrstuv", "mem", "", "#0123456789abcdefghijklmnopqrstuv", ""},
		{"aws://table:bucket/db::foo.foo", "aws", "//table:bucket/db", "foo.foo", ""},
		{"aws://table/db::foo.foo", "aws", "//table/db", "foo.foo", ""},
	}

	for _, tc := range testCases {
		spec, err := ForPath(types.Format_7_18, tc.spec)
		assert.NoError(err)
		defer spec.Close()

		assert.Equal(tc.protocol, spec.Protocol)
		assert.Equal(tc.databaseName, spec.DatabaseName)
		assert.Equal(tc.pathString, spec.Path.String(types.Format_7_18))

		if tc.canonicalSpecIfAny == "" {
			assert.Equal(tc.spec, spec.String(types.Format_7_18))
		} else {
			assert.Equal(tc.canonicalSpecIfAny, spec.String(types.Format_7_18))
		}
	}
}

func TestPinPathSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForPath(types.Format_7_18, "mem::foo.value")
	assert.NoError(err)
	defer unpinned.Close()

	db := unpinned.GetDatabase(context.Background())
	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(42))

	pinned, ok := unpinned.Pin(context.Background(), types.Format_7_18)
	assert.True(ok)
	defer pinned.Close()

	head := db.GetDataset(context.Background(), "foo").Head()

	// TODO(binformat)
	assert.Equal(head.Hash(types.Format_7_18), pinned.Path.Hash)
	assert.Equal(fmt.Sprintf("mem::#%s.value", head.Hash(types.Format_7_18).String()), pinned.String(types.Format_7_18))
	assert.Equal(types.Float(42), pinned.GetValue(context.Background()))
	assert.Equal(types.Float(42), unpinned.GetValue(context.Background()))

	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(43))
	assert.Equal(types.Float(42), pinned.GetValue(context.Background()))
	assert.Equal(types.Float(43), unpinned.GetValue(context.Background()))
}

func TestPinDatasetSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForDataset(types.Format_7_18, "mem::foo")
	assert.NoError(err)
	defer unpinned.Close()

	db := unpinned.GetDatabase(context.Background())
	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(42))

	pinned, ok := unpinned.Pin(context.Background(), types.Format_7_18)
	assert.True(ok)
	defer pinned.Close()

	head := db.GetDataset(context.Background(), "foo").Head()

	commitValue := func(val types.Value) types.Value {
		return val.(types.Struct).Get(datas.ValueField)
	}

	// TODO(binformat)
	assert.Equal(head.Hash(types.Format_7_18), pinned.Path.Hash)
	assert.Equal(fmt.Sprintf("mem::#%s", head.Hash(types.Format_7_18).String()), pinned.String(types.Format_7_18))
	assert.Equal(types.Float(42), commitValue(pinned.GetValue(context.Background())))
	assert.Equal(types.Float(42), unpinned.GetDataset(context.Background()).HeadValue())

	db.CommitValue(context.Background(), db.GetDataset(context.Background(), "foo"), types.Float(43))
	assert.Equal(types.Float(42), commitValue(pinned.GetValue(context.Background())))
	assert.Equal(types.Float(43), unpinned.GetDataset(context.Background()).HeadValue())
}

func TestAlreadyPinnedPathSpec(t *testing.T) {
	assert := assert.New(t)

	unpinned, err := ForPath(types.Format_7_18, "mem::#imgp9mp1h3b9nv0gna6mri53dlj9f4ql.value")
	assert.NoError(err)
	pinned, ok := unpinned.Pin(context.Background(), types.Format_7_18)
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
	// TODO(binformat)
	assert.Equal(s, spec2.GetDatabase(context.Background()).ReadValue(context.Background(), s.Hash(types.Format_7_18)))
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
		_, ok := sp.Pin(context.Background(), types.Format_7_18)
		assert.False(ok)
		assert.Equal(datas.Dataset{}, sp.GetDataset(context.Background()))
		assert.Nil(sp.GetValue(context.Background()))
	}

	test("")
	test("invalid:spec")
	test("💩:spec")
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

	sp, err := ForDataset(types.Format_7_18, "test:foo::bar")
	assert.NoError(err)
	assert.Equal("test", sp.Protocol)
	assert.Equal("foo", sp.DatabaseName)

	cs := sp.NewChunkStore(context.Background())
	assert.Equal("foo", tp.name)
	c := chunks.NewChunk([]byte("hi!"))
	err = cs.Put(context.Background(), c)
	assert.NoError(err)
	ok, err := cs.Has(context.Background(), c.Hash())
	assert.NoError(err)
	assert.True(ok)

	tp.name = ""
	ds := sp.GetDataset(context.Background())
	assert.Equal("foo", tp.name)

	ds, err = ds.Database().CommitValue(context.Background(), ds, types.String("hi!"))
	d.PanicIfError(err)

	assert.True(types.String("hi!").Equals(ds.HeadValue()))
}
