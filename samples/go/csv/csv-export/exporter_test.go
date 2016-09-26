// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"encoding/csv"
	"io"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestCSVExporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
	header  []string
	payload [][]string
}

func createTestData(s *testSuite, buildAsMap bool) []types.Value {
	s.header = []string{"a", "b", "c"}
	structName := "SomeStruct"
	s.payload = [][]string{
		{"4", "10", "255"},
		{"5", "7", "100"},
		{"512", "12", "55"},
	}

	sliceLen := len(s.payload)
	if buildAsMap {
		sliceLen *= 2
	}

	typ := types.MakeStructType(structName, s.header, []*types.Type{
		types.StringType, types.StringType, types.StringType,
	})

	structs := make([]types.Value, sliceLen)
	for i, row := range s.payload {
		fields := make(types.ValueSlice, len(s.header))
		for j, v := range row {
			fields[j] = types.String(v)
		}
		if buildAsMap {
			structs[i*2] = fields[0]
			structs[i*2+1] = types.NewStructWithType(typ, fields)
		} else {
			structs[i] = types.NewStructWithType(typ, fields)
		}
	}
	return structs
}

func verifyOutput(s *testSuite, stdout string) {
	csvReader := csv.NewReader(strings.NewReader(stdout))

	row, err := csvReader.Read()
	d.Chk.NoError(err)
	s.Equal(s.header, row)

	for i := 0; i < len(s.payload); i++ {
		row, err := csvReader.Read()
		d.Chk.NoError(err)
		s.Equal(s.payload[i], row)
	}

	_, err = csvReader.Read()
	s.Equal(io.EOF, err)
}

// FIXME: run with pipe
func (s *testSuite) TestCSVExportFromList() {
	setName := "csvlist"

	// Setup data store
	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	ds := db.GetDataset(setName)

	// Build data rows
	structs := createTestData(s, false)
	db.CommitValue(ds, types.NewList(structs...))
	db.Close()

	// Run exporter
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{dataspec})
	s.Equal("", stderr)

	verifyOutput(s, stdout)
}

func (s *testSuite) TestCSVExportFromMap() {
	setName := "csvmap"

	// Setup data store
	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	ds := db.GetDataset(setName)

	// Build data rows
	structs := createTestData(s, true)
	db.CommitValue(ds, types.NewMap(structs...))
	db.Close()

	// Run exporter
	dataspec := spec.CreateValueSpecString("ldb", s.LdbDir, setName)
	stdout, stderr := s.MustRun(main, []string{dataspec})
	s.Equal("", stderr)

	verifyOutput(s, stdout)
}
