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

package spec

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func mustType(t *types.Type, err error) *types.Type {
	d.PanicIfError(err)
	return t
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustList(l types.List, err error) types.List {
	d.PanicIfError(err)
	return l
}

func mustHash(h hash.Hash, err error) hash.Hash {
	d.PanicIfError(err)
	return h
}

func mustGetValue(v types.Value, found bool, err error) types.Value {
	d.PanicIfError(err)
	d.PanicIfFalse(found)
	return v
}

func TestMemDatabaseSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForDatabase("mem")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.True(spec.Path.IsEmpty())

	s := types.String("hello")
	vrw := spec.GetVRW(context.Background())
	vrw.WriteValue(context.Background(), s)
	assert.Equal(s, mustValue(vrw.ReadValue(context.Background(), mustHash(s.Hash(vrw.Format())))))
}

func TestMemDatasetSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForDataset("mem::test")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.Equal("test", spec.Path.Dataset)

	ds := spec.GetDataset(context.Background())
	_, ok, err := spec.GetDataset(context.Background()).MaybeHeadValue()
	assert.NoError(err)
	assert.False(ok)

	s := types.String("hello")
	db := spec.GetDatabase(context.Background())
	ds, err = datas.CommitValue(context.Background(), db, ds, s)
	assert.NoError(err)
	currHeadVal, ok, err := ds.MaybeHeadValue()
	assert.NoError(err)
	assert.True(ok)
	assert.Equal(s, currHeadVal)
}

func TestMemHashPathSpec(t *testing.T) {
	assert := assert.New(t)

	s := types.String("hello")

	spec, err := ForPath("mem::#" + mustHash(s.Hash(types.Format_Default)).String())
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.False(spec.Path.IsEmpty())

	// This is a reasonable check but it causes the next GetValue to return nil:
	// assert.Nil(spec.GetValue())

	spec.GetVRW(context.Background()).WriteValue(context.Background(), s)
	value, err := spec.GetValue(context.Background())
	assert.NoError(err)
	assert.Equal(s, value)
}

func TestMemDatasetPathSpec(t *testing.T) {
	assert := assert.New(t)

	spec, err := ForPath("mem::test")
	assert.NoError(err)
	defer spec.Close()

	assert.Equal("mem", spec.Protocol)
	assert.Equal("", spec.DatabaseName)
	assert.False(spec.Path.IsEmpty())

	assert.Nil(spec.GetValue(context.Background()))

	db := spec.GetDatabase(context.Background())
	ds, err := db.GetDataset(context.Background(), "test")
	assert.NoError(err)
	_, err = datas.CommitValue(context.Background(), db, ds, mustList(types.NewList(context.Background(), spec.GetVRW(context.Background()), types.Float(42))))
	assert.NoError(err)

	value, err := spec.GetValue(context.Background())
	require.NoError(t, err)
	assert.NotNil(value)
}

func TestNBSDatabaseSpec(t *testing.T) {
	assert := assert.New(t)

	run := func(prefix string) {
		tmpDir, err := os.MkdirTemp("", "spec_test")
		assert.NoError(err)
		defer file.RemoveAll(tmpDir)

		s := types.String("string")

		// Existing database in the database are read from the spec.
		store1 := filepath.Join(tmpDir, "store1")
		os.Mkdir(store1, 0777)
		func() {
			cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), store1, 8*(1<<20), nbs.NewUnlimitedMemQuotaProvider())
			assert.NoError(err)
			vrw := types.NewValueStore(cs)
			db := datas.NewTypesDatabase(vrw, tree.NewNodeStore(cs))
			defer db.Close()
			r, err := vrw.WriteValue(context.Background(), s)
			assert.NoError(err)
			ds, err := db.GetDataset(context.Background(), "datasetID")
			assert.NoError(err)
			_, err = datas.CommitValue(context.Background(), db, ds, r)
			assert.NoError(err)
		}()

		spec1, err := ForDatabase(prefix + store1)
		assert.NoError(err)
		defer spec1.Close()

		assert.Equal("nbs", spec1.Protocol)
		assert.Equal(store1, spec1.DatabaseName)

		vrw := spec1.GetVRW(context.Background())

		assert.Equal(s, mustValue(vrw.ReadValue(context.Background(), mustHash(s.Hash(vrw.Format())))))

		// New databases can be created and read/written from.
		store2 := filepath.Join(tmpDir, "store2")
		os.Mkdir(store2, 0777)
		spec2, err := ForDatabase(prefix + store2)
		assert.NoError(err)
		defer spec2.Close()

		assert.Equal("nbs", spec2.Protocol)
		assert.Equal(store2, spec2.DatabaseName)

		db := spec2.GetDatabase(context.Background())
		vrw = spec2.GetVRW(context.Background())
		vrw.WriteValue(context.Background(), s)
		r, err := vrw.WriteValue(context.Background(), s)
		assert.NoError(err)
		ds, err := db.GetDataset(context.Background(), "datasetID")
		assert.NoError(err)
		_, err = datas.CommitValue(context.Background(), db, ds, r)
		assert.NoError(err)
		assert.Equal(s, mustValue(vrw.ReadValue(context.Background(), mustHash(s.Hash(vrw.Format())))))
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
	sp, _ = ForDataset("aws://[table:bucket]/foo/bar/baz::myds")
	assert.Equal("aws://[table:bucket]/foo/bar/baz", sp.Href())
	sp, _ = ForPath("aws://[table:bucket]/foo/bar/baz::myds.my.path")
	assert.Equal("aws://[table:bucket]/foo/bar/baz", sp.Href())

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
		"ldb:",
		"random:",
		"random:random",
		"/file/ba:d",
		"aws://[t:b]",
		"aws://t",
		"aws://t:",
	}

	for _, spec := range badSpecs {
		_, err := ForDatabase(spec)
		assert.Error(err, spec)
	}

	tmpDir, err := os.MkdirTemp("", "spec_test")
	assert.NoError(err)
	defer file.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, canonicalSpecIfAny string
	}{
		{"mem", "mem", "", ""},
		{tmpDir, "nbs", tmpDir, "nbs:" + tmpDir},
		{"nbs:" + tmpDir, "nbs", tmpDir, ""},
		{"aws://[table:bucket]/db", "aws", "//[table:bucket]/db", ""},
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
	badSpecs := []string{
		"mem",
		"mem:",
		"mem:::ds",
		"monkey",
		"monkey:balls",
		"mem:/a/bogus/path:dsname",
		"nbs:",
		"nbs:hello",
		"aws://[t:b]/db",
	}

	for _, spec := range badSpecs {
		t.Run(spec, func(t *testing.T) {
			_, err := ForDataset(spec)
			assert.Error(t, err, spec)
		})
	}

	validDatasetNames := []string{"a", "Z", "0", "/", "-", "_"}
	for _, s := range validDatasetNames {
		_, err := ForDataset("mem::" + s)
		assert.NoError(t, err)
	}

	tmpDir, err := os.MkdirTemp("", "spec_test")
	assert.NoError(t, err)
	defer file.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, datasetName, canonicalSpecIfAny string
	}{
		{"nbs:" + tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", ""},
		{tmpDir + "::ds/one", "nbs", tmpDir, "ds/one", "nbs:" + tmpDir + "::ds/one"},
		{"aws://[table:bucket]/db::ds", "aws", "//[table:bucket]/db", "ds", ""},
		{"aws://table/db::ds", "aws", "//table/db", "ds", ""},
	}

	for _, tc := range testCases {
		assert := assert.New(t)
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

	tmpDir, err := os.MkdirTemp("", "spec_test")
	assert.NoError(err)
	defer file.RemoveAll(tmpDir)

	testCases := []struct {
		spec, protocol, databaseName, pathString, canonicalSpecIfAny string
	}{
		{tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", "nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv"},
		{"nbs:" + tmpDir + "::#0123456789abcdefghijklmnopqrstuv", "nbs", tmpDir, "#0123456789abcdefghijklmnopqrstuv", ""},
		{"mem::#0123456789abcdefghijklmnopqrstuv", "mem", "", "#0123456789abcdefghijklmnopqrstuv", ""},
		{"aws://[table:bucket]/db::foo.foo", "aws", "//[table:bucket]/db", "foo.foo", ""},
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

func TestMultipleSpecsSameNBS(t *testing.T) {
	assert := assert.New(t)

	tmpDir, err := os.MkdirTemp("", "spec_test")
	assert.NoError(err)
	defer file.RemoveAll(tmpDir)

	spec1, err1 := ForDatabase(tmpDir)
	spec2, err2 := ForDatabase(tmpDir)

	assert.NoError(err1)
	assert.NoError(err2)

	s := types.String("hello")
	db := spec1.GetDatabase(context.Background())
	vrw := spec1.GetVRW(context.Background())
	r, err := vrw.WriteValue(context.Background(), s)
	assert.NoError(err)
	ds, err := db.GetDataset(context.Background(), "datasetID")
	assert.NoError(err)
	_, err = datas.CommitValue(context.Background(), db, ds, r)
	assert.NoError(err)
	assert.Equal(s, mustValue(spec2.GetVRW(context.Background()).ReadValue(context.Background(), mustHash(s.Hash(vrw.Format())))))
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

	sp, err := ForDataset("test:foo::bar")
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

	ds, err = datas.CommitValue(context.Background(), ds.Database(), ds, types.String("hi!"))
	d.PanicIfError(err)

	headVal, ok, err := ds.MaybeHeadValue()
	assert.NoError(err)
	assert.True(ok)
	assert.True(types.String("hi!").Equals(headVal))
}
