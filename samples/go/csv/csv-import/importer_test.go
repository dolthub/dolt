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

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/stretchr/testify/suite"
)

const (
	TEST_DATA_SIZE = 100
	TEST_YEAR      = 2012
	TEST_FIELDS    = "Float,String,Float,Float"
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
	writeCSVWithHeader(w, "year,a,b,c\n", 0)
}

func writeCSVWithHeader(w io.Writer, header string, startingValue int) {
	_, err := io.WriteString(w, header)
	d.Chk.NoError(err)
	for i := 0; i < TEST_DATA_SIZE; i++ {
		j := i + startingValue
		_, err = io.WriteString(w, fmt.Sprintf("%d,a%d,%d,%d\n", TEST_YEAR+j%3, j, j, j*2))
		d.Chk.NoError(err)
	}
}

func (s *testSuite) validateList(l types.List) {
	s.Equal(uint64(TEST_DATA_SIZE), l.Len())

	i := uint64(0)
	l.IterAll(func(v types.Value, j uint64) {
		s.Equal(i, j)
		st := v.(types.Struct)
		s.Equal(types.Float(TEST_YEAR+i%3), st.Get("year"))
		s.Equal(types.String(fmt.Sprintf("a%d", i)), st.Get("a"))
		s.Equal(types.Float(i), st.Get("b"))
		s.Equal(types.Float(i*2), st.Get("c"))
		i++
	})
}

func (s *testSuite) validateMap(vrw types.ValueReadWriter, m types.Map) {
	// --dest-type=map:1 so key is field "a"
	s.Equal(uint64(TEST_DATA_SIZE), m.Len())

	for i := 0; i < TEST_DATA_SIZE; i++ {
		v := m.Get(types.String(fmt.Sprintf("a%d", i))).(types.Struct)
		s.True(v.Equals(
			types.NewStruct("Row", types.StructData{
				"year": types.Float(TEST_YEAR + i%3),
				"a":    types.String(fmt.Sprintf("a%d", i)),
				"b":    types.Float(i),
				"c":    types.Float(i * 2),
			})))
	}
}

func (s *testSuite) validateNestedMap(vrw types.ValueReadWriter, m types.Map) {
	// --dest-type=map:0,1 so keys are fields "year", then field "a"
	s.Equal(uint64(3), m.Len())

	for i := 0; i < TEST_DATA_SIZE; i++ {
		n := m.Get(types.Float(TEST_YEAR + i%3)).(types.Map)
		o := n.Get(types.String(fmt.Sprintf("a%d", i))).(types.Struct)
		s.True(o.Equals(types.NewStruct("Row", types.StructData{
			"year": types.Float(TEST_YEAR + i%3),
			"a":    types.String(fmt.Sprintf("a%d", i)),
			"b":    types.Float(i),
			"c":    types.Float(i * 2),
		})))
	}
}

func (s *testSuite) validateColumnar(vrw types.ValueReadWriter, str types.Struct, reps int) {
	s.Equal("Columnar", str.Name())

	lists := map[string]types.List{}
	for _, nm := range []string{"year", "a", "b", "c"} {
		l := str.Get(nm).(types.Ref).TargetValue(vrw).(types.List)
		s.Equal(uint64(reps*TEST_DATA_SIZE), l.Len())
		lists[nm] = l
	}

	for i := 0; i < reps*TEST_DATA_SIZE; i++ {
		s.Equal(types.Float(TEST_YEAR+i%3), lists["year"].Get(uint64(i)))
		s.Equal(types.String(fmt.Sprintf("a%d", i)), lists["a"].Get(uint64(i)))
		s.Equal(types.Float(i), lists["b"].Get(uint64(i)))
		s.Equal(types.Float(i*2), lists["c"].Get(uint64(i)))
	}
}

func (s *testSuite) TestCSVImporter() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	s.validateList(ds.HeadValue().(types.List))
}

func (s *testSuite) TestCSVImporterLowercase() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	writeCSVWithHeader(input, "YeAr,a,B,c\n", 0)
	defer os.Remove(input.Name())

	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--lowercase", "--column-types", TEST_FIELDS, input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	s.validateList(ds.HeadValue().(types.List))
}

func (s *testSuite) TestCSVImporterLowercaseDuplicate() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	writeCSVWithHeader(input, "YeAr,a,B,year\n", 0)
	defer os.Remove(input.Name())

	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	_, stderr, _ := s.Run(main, []string{"--no-progress", "--lowercase", "--column-types", TEST_FIELDS, input.Name(), dataspec})
	s.Contains(stderr, "must be unique")
}

func (s *testSuite) TestCSVImporterFromBlob() {
	test := func(pathFlag string) {
		defer os.RemoveAll(s.DBDir)

		newDB := func() datas.Database {
			os.Mkdir(s.DBDir, 0777)
			cs := nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize)
			return datas.NewDatabase(cs)
		}

		db := newDB()
		rawDS := db.GetDataset("raw")
		csv := &bytes.Buffer{}
		writeCSV(csv)
		db.CommitValue(rawDS, types.NewBlob(db, csv))
		db.Close()

		stdout, stderr := s.MustRun(main, []string{
			"--no-progress", "--column-types", TEST_FIELDS,
			pathFlag, spec.CreateValueSpecString("nbs", s.DBDir, "raw.value"),
			spec.CreateValueSpecString("nbs", s.DBDir, "csv"),
		})
		s.Equal("", stdout)
		s.Equal("", stderr)

		db = newDB()
		defer db.Close()
		csvDS := db.GetDataset("csv")
		s.validateList(csvDS.HeadValue().(types.List))
	}
	test("--path")
	test("-p")
}

func (s *testSuite) TestCSVImporterToMap() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:1", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	s.validateMap(db, m)
}

func (s *testSuite) TestCSVImporterToNestedMap() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:0,1", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	s.validateNestedMap(db, m)
}

func (s *testSuite) TestCSVImporterToNestedMapByName() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", TEST_FIELDS, "--dest-type", "map:year,a", s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	m := ds.HeadValue().(types.Map)
	s.validateNestedMap(db, m)
}

func (s *testSuite) TestCSVImporterToColumnar() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--invert", "--column-types", TEST_FIELDS, s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	str := ds.HeadValue().(types.Struct)
	s.validateColumnar(db, str, 1)
}

func (s *testSuite) TestCSVImporterToColumnarAppend() {
	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--invert", "--column-types", TEST_FIELDS, s.tmpFileName, dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	writeCSVWithHeader(input, "year,a,b,c\n", 100)
	defer os.Remove(input.Name())

	stdout, stderr = s.MustRun(main, []string{"--no-progress", "--invert", "--append", "--column-types", TEST_FIELDS, input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	str := ds.HeadValue().(types.Struct)
	s.validateColumnar(db, str, 2)
}

func (s *testSuite) TestCSVImporterWithPipe() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("a|b\n1|2\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", "String,Float", "--delimiter", "|", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("1"), st.Get("a"))
	s.Equal(types.Float(2), st.Get("b"))
}

func (s *testSuite) TestCSVImporterWithExternalHeader() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--column-types", "String,Float", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.Float(8), st.Get("y"))
}

func (s *testSuite) TestCSVImporterWithInvalidExternalHeader() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("7#8\n")
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr, exitErr := s.Run(main, []string{"--no-progress", "--column-types", "String,Float", "--header", "x,x", input.Name(), dataspec})
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
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
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
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)

	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--skip-records", "2", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
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
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)

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
	dataspec := spec.CreateValueSpecString("nbs", s.DBDir, setName)
	stdout, stderr := s.MustRun(main, []string{"--no-progress", "--skip-records", "1", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	defer os.RemoveAll(s.DBDir)
	defer db.Close()
	ds := db.GetDataset(setName)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.String("8"), st.Get("y"))
}
