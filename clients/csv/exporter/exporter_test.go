package main

import (
	"encoding/csv"
	"io"
	"strings"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestCSVExporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	util.ClientTestSuite
}

// FIXME: run with pipe
func (s *testSuite) TestCSVExporter() {
	storeName := "store"
	setName := "csv"
	header := []string{"a", "b", "c"}
	payload := [][]string{
		[]string{"5", "7", "100"},
		[]string{"4", "10", "255"},
		[]string{"512", "12", "55"},
	}
	structName := "SomeStruct"

	// Setup data store
	cs := chunks.NewLevelDBStore(s.LdbDir, storeName, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), setName)

	// Build Struct fields based on header
	f := make([]types.Field, 0, len(header))
	for _, key := range header {
		f = append(f, types.Field{
			Name: key,
			Type: types.StringType,
		})
	}

	typ := types.MakeStructType(structName, f)
	structFields := typ.Desc.(types.StructDesc).Fields

	// Build data rows
	structs := make([]types.Value, len(payload))
	for i, row := range payload {
		fields := make(map[string]types.Value)
		for j, v := range row {
			fields[structFields[j].Name] = types.NewString(v)
		}
		structs[i] = types.NewStruct(typ, fields)
	}

	listType := types.MakeListType(typ)
	ds.Commit(types.NewTypedList(listType, structs...))
	ds.Store().Close()

	// Run exporter
	out := s.Run(main, []string{"-store", storeName, "-ds", setName})

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

	row, err = csvReader.Read()
	s.Equal(io.EOF, err)
}
