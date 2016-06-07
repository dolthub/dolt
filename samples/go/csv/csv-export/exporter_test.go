// Copyright 2016 The Noms Authors. All rights reserved.
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
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/test_util"
	"github.com/attic-labs/testify/suite"
)

func TestCSVExporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	test_util.ClientTestSuite
}

// FIXME: run with pipe
func (s *testSuite) TestCSVExporter() {
	setName := "csv"
	header := []string{"a", "b", "c"}
	payload := [][]string{
		[]string{"5", "7", "100"},
		[]string{"4", "10", "255"},
		[]string{"512", "12", "55"},
	}
	structName := "SomeStruct"

	// Setup data store
	cs := chunks.NewLevelDBStore(s.LdbDir, "", 1, false)
	ds := dataset.NewDataset(datas.NewDatabase(cs), setName)

	// Build Struct fields based on header
	f := make(types.TypeMap, len(header))
	for _, key := range header {
		f[key] = types.StringType
	}

	typ := types.MakeStructType(structName, f)

	// Build data rows
	structs := make([]types.Value, len(payload))
	for i, row := range payload {
		fields := make(map[string]types.Value)
		for j, v := range row {
			name := header[j]
			fields[name] = types.NewString(v)
		}
		structs[i] = types.NewStructWithType(typ, fields)
	}

	ds.Commit(types.NewList(structs...))
	ds.Database().Close()

	// Run exporter
	dataspec := test_util.CreateValueSpecString("ldb", s.LdbDir, setName)
	out := s.Run(main, []string{dataspec})

	// Verify output
	csvReader := csv.NewReader(strings.NewReader(out))

	row, err := csvReader.Read()
	d.Chk.NoError(err)
	s.Equal(header, row)

	for i := 0; i < len(payload); i++ {
		row, err := csvReader.Read()
		d.Chk.NoError(err)
		s.Equal(payload[i], row)
	}

	_, err = csvReader.Read()
	s.Equal(io.EOF, err)
}
