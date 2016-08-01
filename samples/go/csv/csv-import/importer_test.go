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
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestCSVImporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func writeCSV(w io.Writer) {
	_, err := io.WriteString(w, "a,b\n")
	d.Chk.NoError(err)
	for i := 0; i < 100; i++ {
		_, err = io.WriteString(w, fmt.Sprintf("a%d,%d\n", i, i))
		d.Chk.NoError(err)
	}
}

func validateCSV(s *testSuite, l types.List) {
	s.Equal(uint64(100), l.Len())

	i := uint64(0)
	l.IterAll(func(v types.Value, j uint64) {
		s.Equal(i, j)
		st := v.(types.Struct)
		s.Equal(types.String(fmt.Sprintf("a%d", i)), st.Get("a"))
		s.Equal(types.Number(i), st.Get("b"))
		i++
	})
}

func (s *testSuite) TestCSVImporter() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	writeCSV(input)
	defer input.Close()
	defer os.Remove(input.Name())

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.Run(main, []string{"--no-progress", "--column-types", "String,Number", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

	validateCSV(s, ds.HeadValue().(types.List))
}

func (s *testSuite) TestCSVImporterFromBlob() {
	test := func(pathFlag string) {
		defer os.RemoveAll(s.LdbDir)

		newDB := func() datas.Database {
			cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
			return datas.NewDatabase(cs)
		}

		db := newDB()
		rawDS := dataset.NewDataset(db, "raw")
		csv := &bytes.Buffer{}
		writeCSV(csv)
		rawDS.CommitValue(types.NewBlob(csv))
		db.Close()

		stdout, stderr := s.Run(main, []string{
			"--no-progress", "--column-types", "String,Number",
			pathFlag, spec.CreateValueSpecString("ldb", s.LdbDir, "raw.value"),
			spec.CreateValueSpecString("ldb", s.LdbDir, "csv"),
		})
		s.Equal("", stdout)
		s.Equal("", stderr)

		db = newDB()
		defer db.Close()
		csvDS := dataset.NewDataset(db, "csv")
		validateCSV(s, csvDS.HeadValue().(types.List))
	}
	test("--path")
	test("-p")
}

func (s *testSuite) TestCSVImporterToMap() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	defer input.Close()
	defer os.Remove(input.Name())

	_, err = input.WriteString("a,b,c\n")
	d.Chk.NoError(err)
	for i := 0; i < 20; i++ {
		_, err = input.WriteString(fmt.Sprintf("a%d,%d,%d\n", i, i, i*2))
		d.Chk.NoError(err)
	}
	_, err = input.Seek(0, 0)
	d.Chk.NoError(err)

	setName := "csv"
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.Run(main, []string{"--no-progress", "--column-types", "String,Number,Number", "--dest-type", "map:1", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

	m := ds.HeadValue().(types.Map)
	s.Equal(uint64(20), m.Len())

	for i := 0; i < 20; i++ {
		m.Get(types.Number(i)).(types.Struct).Equals(types.NewStruct("", types.StructData{
			"a": types.String(fmt.Sprintf("a%d", i)),
			"c": types.Number(i * 2),
		}))
	}
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
	stdout, stderr := s.Run(main, []string{"--no-progress", "--column-types", "String,Number", "--delimiter", "|", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

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
	stdout, stderr := s.Run(main, []string{"--no-progress", "--column-types", "String,Number", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.Number(8), st.Get("y"))
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
	stdout, stderr := s.Run(main, []string{"--no-progress", "--skip-records", "2", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("a"))
	s.Equal(types.String("8"), st.Get("b"))
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
	stdout, stderr := s.Run(main, []string{"--no-progress", "--skip-records", "1", "--header", "x,y", input.Name(), dataspec})
	s.Equal("", stdout)
	s.Equal("", stderr)

	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)
	defer ds.Database().Close()
	defer os.RemoveAll(s.LdbDir)

	l := ds.HeadValue().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := v.(types.Struct)
	s.Equal(types.String("7"), st.Get("x"))
	s.Equal(types.String("8"), st.Get("y"))
}
