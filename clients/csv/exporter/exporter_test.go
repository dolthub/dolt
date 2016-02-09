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
	"github.com/attic-labs/noms/ref"
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
			T:    types.MakePrimitiveType(types.StringKind),
		})
	}

	typeDef := types.MakeStructType(structName, f, types.Choices{})
	pkg := types.NewPackage([]types.Type{typeDef}, []ref.Ref{})
	pkgRef := types.RegisterPackage(&pkg)
	typeRef := types.MakeType(pkgRef, 0)
	structFields := typeDef.Desc.(types.StructDesc).Fields

	// Build data rows
	refs := make([]types.Value, 0, len(payload))
	for i := 0; i < len(payload); i++ {
		fields := make(map[string]types.Value)
		for j, v := range payload[i] {
			fields[structFields[j].Name] = types.NewString(v)
		}
		newStruct := types.NewStruct(typeRef, typeDef, fields)
		r := types.NewRef(types.WriteValue(newStruct, cs))
		refs = append(refs, r)
	}

	refType := types.MakeCompoundType(types.RefKind, typeRef)
	listType := types.MakeCompoundType(types.ListKind, refType)
	ds.Commit(types.NewTypedList(listType, refs...))
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
