// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

const (
	TEST_ROW_STRUCT_NAME = "row"
	TEST_ROW_FIELDS      = "anid,month,rainfall,year"
	TEST_DATA_SIZE       = 200
	TEST_YEAR            = 2012
)

func TestCSVWrite(t *testing.T) {
	suite.Run(t, &csvWriteTestSuite{})
}

type csvWriteTestSuite struct {
	clienttest.ClientTestSuite
	fieldTypes    []*types.Type
	rowStructDesc types.StructDesc
	comma         rune
	tmpFileName   string
}

func typesToKinds(ts []*types.Type) KindSlice {
	kinds := make(KindSlice, len(ts))
	for i, t := range ts {
		kinds[i] = t.Kind()
	}
	return kinds
}

func (s *csvWriteTestSuite) SetupTest() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)
	s.tmpFileName = input.Name()
	defer input.Close()

	fieldNames := strings.Split(TEST_ROW_FIELDS, ",")
	s.fieldTypes = []*types.Type{types.StringType, types.NumberType, types.NumberType, types.NumberType}
	rowStructType := types.MakeStructType(TEST_ROW_STRUCT_NAME, fieldNames, s.fieldTypes)
	s.rowStructDesc = rowStructType.Desc.(types.StructDesc)
	s.comma, _ = StringToRune(",")
	createCsvTestExpectationFile(input)
}

func (s *csvWriteTestSuite) TearDownTest() {
	os.Remove(s.tmpFileName)
}

func createCsvTestExpectationFile(w io.Writer) {
	_, err := io.WriteString(w, TEST_ROW_FIELDS)
	d.Chk.NoError(err)
	_, err = io.WriteString(w, "\n")
	d.Chk.NoError(err)
	for i := 0; i < TEST_DATA_SIZE; i++ {
		_, err = io.WriteString(w, fmt.Sprintf("a - %3d,%d,%d,%d\n", i, i%12, i%32, TEST_YEAR+i%4))
		d.Chk.NoError(err)
	}
}

func startReadingCsvTestExpectationFile(s *csvWriteTestSuite) (cr *csv.Reader, headers []string) {
	res, err := os.Open(s.tmpFileName)
	d.PanicIfError(err)
	cr = NewCSVReader(res, s.comma)
	headers, err = cr.Read()
	d.PanicIfError(err)
	return
}

func createTestList(s *csvWriteTestSuite) types.List {
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	cr, headers := startReadingCsvTestExpectationFile(s)
	l, _ := ReadToList(cr, TEST_ROW_STRUCT_NAME, headers, typesToKinds(s.fieldTypes), ds)
	return l
}

func createTestMap(s *csvWriteTestSuite) types.Map {
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	cr, headers := startReadingCsvTestExpectationFile(s)
	return ReadToMap(cr, TEST_ROW_STRUCT_NAME, headers, []string{"anid"}, typesToKinds(s.fieldTypes), ds)
}

func createTestNestedMap(s *csvWriteTestSuite) types.Map {
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	cr, headers := startReadingCsvTestExpectationFile(s)
	return ReadToMap(cr, TEST_ROW_STRUCT_NAME, headers, []string{"anid", "year"}, typesToKinds(s.fieldTypes), ds)
}

func verifyOutput(s *csvWriteTestSuite, r io.Reader) {
	res, err := os.Open(s.tmpFileName)
	d.PanicIfError(err)
	actual, err := ioutil.ReadAll(r)
	d.Chk.NoError(err)
	expected, err := ioutil.ReadAll(res)
	d.Chk.NoError(err)
	s.True(string(expected) == string(actual), "csv files are different")
}

func (s *csvWriteTestSuite) TestCSVWriteList() {
	l := createTestList(s)
	w := new(bytes.Buffer)
	s.True(TEST_DATA_SIZE == l.Len(), "list length")
	WriteList(l, s.rowStructDesc, s.comma, w)
	verifyOutput(s, w)
}

func (s *csvWriteTestSuite) TestCSVWriteMap() {
	m := createTestMap(s)
	w := new(bytes.Buffer)
	s.True(TEST_DATA_SIZE == m.Len(), "map length")
	WriteMap(m, s.rowStructDesc, s.comma, w)
	verifyOutput(s, w)
}

func (s *csvWriteTestSuite) TestCSVWriteNestedMap() {
	m := createTestNestedMap(s)
	w := new(bytes.Buffer)
	s.True(TEST_DATA_SIZE == m.Len(), "nested map length")
	WriteMap(m, s.rowStructDesc, s.comma, w)
	verifyOutput(s, w)
}
