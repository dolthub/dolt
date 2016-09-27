// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

const (
	TEST_DATA_SIZE = 100
	TEST_YEAR      = 2012
	TEST_FIELDS    = "Number,String,Number,Number"
)

func TestCSVImporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
	tmpFileName string
}

func (s *testSuite) SetupTest() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	s.tmpFileName = input.Name()
	writeCSV(input)
}

func (s *testSuite) TearDownTest() {
	os.Remove(s.tmpFileName)
}

func writeCSV(w io.Writer) {
	_, err := io.WriteString(w, "year,a,b,c\n")
	d.Chk.NoError(err)
	for i := 0; i < TEST_DATA_SIZE; i++ {
		_, err = io.WriteString(w, fmt.Sprintf("%d,a%d,%d,%d\n", TEST_YEAR+i%3, i, i, i*2))
		d.Chk.NoError(err)
	}
}

func validateList(s *testSuite, l types.List) {
	s.Equal(uint64(TEST_DATA_SIZE), l.Len())

	i := uint64(0)
	l.IterAll(func(v types.Value, j uint64) {
		s.Equal(i, j)
		st := v.(types.Struct)
		s.Equal(types.Number(TEST_YEAR+i%3), st.Get("year"))
		s.Equal(types.String(fmt.Sprintf("a%d", i)), st.Get("a"))
		s.Equal(types.Number(i), st.Get("b"))
		s.Equal(types.Number(i*2), st.Get("c"))
		i++
	})
}

func validateMap(s *testSuite, m types.Map) {
	// --dest-type=map:1 so key is field "a"
	s.Equal(uint64(TEST_DATA_SIZE), m.Len())

	for i := 0; i < TEST_DATA_SIZE; i++ {
		v := m.Get(types.String(fmt.Sprintf("a%d", i))).(types.Struct)
		s.True(v.Equals(
			types.NewStruct("Row", types.StructData{
				"year": types.Number(TEST_YEAR + i%3),
				"a":    types.String(fmt.Sprintf("a%d", i)),
				"b":    types.Number(i),
				"c":    types.Number(i * 2),
			})))
	}
}

func validateNestedMap(s *testSuite, m types.Map) {
	// --dest-type=map:0,1 so keys are fields "year", then field "a"
	s.Equal(uint64(3), m.Len())

	for i := 0; i < TEST_DATA_SIZE; i++ {
		n := m.Get(types.Number(TEST_YEAR + i%3)).(types.Map)
		o := n.Get(types.String(fmt.Sprintf("a%d", i))).(types.Struct)
		s.True(o.Equals(types.NewStruct("Row", types.StructData{
			"year": types.Number(TEST_YEAR + i%3),
			"a":    types.String(fmt.Sprintf("a%d", i)),
			"b":    types.Number(i),
			"c":    types.Number(i * 2),
		})))
	}
}

func (s *testSuite) TestCSVImporter() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	validateList(s, ds.HeadValue().(types.List))
}

func (s *testSuite) TestCSVImporterFromBlob() {
	test := func(pathFlag string) {
		defer os.RemoveAll(s.LdbDir)

		newDB := func() datas.Database {
			cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
			return datas.NewDatabase(cs)
		}

		db := newDB()
		rawDS := db.GetDataset("raw")
		csv := &bytes.Buffer{}
		writeCSV(csv)
		db.CommitValue(rawDS, types.NewBlob(csv))
		db.Close()

		stdout, stderr := s.MustRun(main, []string{
			"--no-progress", "--column-types", TEST_FIELDS,
			pathFlag, spec.CreateValueSpecString("ldb", s.LdbDir, "raw.value"),
			spec.CreateValueSpecString("ldb", s.LdbDir, "csv"),
		})
		s.Equal("", stdout)
		s.Equal("", stderr)

		db = newDB()
		defer db.Close()
		csvDS := db.GetDataset("csv")
		validateList(s, csvDS.HeadValue().(types.List))
	}
	test("--path")
	test("-p")
}

func (s *testSuite) TestCSVImporterToMap() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:1", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	validateMap(s, m)
}

func (s *testSuite) TestCSVImporterToNestedMap() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:0,1", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	validateNestedMap(s, m)
}

func (s *testSuite) TestCSVImporterToNestedMapByName() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:year,a", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	validateNestedMap(s, m)
}

func (s *testSuite) TestCSVImporterWithPipe() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("a|b\n1|2\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", "String,Number", "--delimiter", "|", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("1"), st.Get("a"))
	s.Equal(types.Number(2), st.Get("b"))
}

func (s *testSuite) TestCSVImporterWithExternalHeader() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", "String,Number", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.Number(8), st.Get("y"))
}

func (s *testSuite) TestCSVImporterWithInvalidExternalHeader() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("7#8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr, exitErr := s.Run(main, []string{"--no-progress", "--column-types", "String,Number", "--header", "x,x", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("error: Invalid headers specified, headers must be unique\n", stderr)
	s.Equal(clienttest.ExitError{1}, exitErr)
}

func (s *testSuite) TestCSVImporterWithInvalidNumColumnTypeSpec() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr, exitErr := s.Run(main, []string{"--no-progress", "--column-types", "String", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("error: Invalid column-types specified, column types do not correspond to number of headers\n", stderr)
	s.Equal(clienttest.ExitError{1}, exitErr)
}

func (s *testSuite) TestCSVImportSkipRecords() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("garbage foo\n")
	d.Chk.NoError(err)

	_, err = input.WriteString("garbage bar\n")
	d.Chk.NoError(err)

	_, err = input.WriteString("a,b\n")
	d.Chk.NoError(err)

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)

	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--skip-records", "2", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("a"))
	s.Equal(types.String("8"), st.Get("b"))
}

func (s *testSuite) TestCSVImportSkipRecordsTooMany() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("a,b\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)

	stdout, stderr, recoveredErr := s.Run(main, []string{"--no-progress", "--skip-records", "100", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("error: skip-records skipped past EOF\n", stderr)
	s.Equal(clienttest.ExitError{1}, recoveredErr)

}

func (s *testSuite) TestCSVImportSkipRecordsCustomHeader() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("a,b\n")
	d.Chk.NoError(err)

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--skip-records", "1", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	defer os.RemoveAll(s.LdbDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.String("8"), st.Get("y"))
}
