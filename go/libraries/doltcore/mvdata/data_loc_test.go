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

package mvdata

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	testTableName      = "test_table"
	testSchemaFileName = "schema.sql"
	testSchema         = `
CREATE TABLE test_table (
	a VARCHAR(120) COMMENT 'tag:0',
	b VARCHAR(120) COMMENT 'tag:1',
	PRIMARY KEY(a)
);`
)

var rowMap = []map[string]interface{}{
	{"a": []string{"a", "b", "c"}},
	{"b": []string{"1", "2", "3"}},
}

func createRootAndFS() (*doltdb.DoltDB, *doltdb.RootValue, filesys.Filesys) {

	testHomeDir := "/user/bheni"
	workingDir := "/user/bheni/datasets/states"
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	fs.WriteFile(testSchemaFileName, []byte(testSchema))
	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.InMemDoltDB, filesys.LocalFS)
	ddb.WriteEmptyRepo(context.Background(), "billy bob", "bigbillieb@fake.horse")

	cs, _ := doltdb.NewCommitSpec("master")
	commit, _ := ddb.Resolve(context.Background(), cs, nil)
	root, err := commit.GetRootValue()

	if err != nil {
		panic(err)
	}

	return ddb, root, fs
}

func TestBasics(t *testing.T) {
	tests := []struct {
		dl                 DataLocation
		expectedStr        string
		expectedIsFileType bool
	}{
		{NewDataLocation("", ".csv"), "stream", false},
		{NewDataLocation(testTableName, ""), DoltDB.ReadableStr() + ":" + testTableName, false},
		{NewDataLocation("file.csv", ""), CsvFile.ReadableStr() + ":file.csv", true},
		{NewDataLocation("file.psv", ""), PsvFile.ReadableStr() + ":file.psv", true},
		{NewDataLocation("file.json", ""), JsonFile.ReadableStr() + ":file.json", true},
		//{NewDataLocation("file.nbf", ""), NbfFile, "file.nbf", true},
	}

	for _, test := range tests {
		t.Run(test.dl.String(), func(t *testing.T) {
			assert.Equal(t, test.expectedStr, test.dl.String())

			_, isFileType := test.dl.(FileDataLocation)
			assert.Equal(t, test.expectedIsFileType, isFileType)
		})
	}
}

var fakeFields = schema.NewColCollection(
	schema.NewColumn("a", 0, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn("b", 1, types.StringKind, false),
)

func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

var fakeSchema schema.Schema
var imt *table.InMemTable
var imtRows []row.Row

func init() {
	fakeSchema = schema.MustSchemaFromCols(fakeFields)

	imtRows = []row.Row{
		mustRow(row.New(types.Format_Default, fakeSchema, row.TaggedValues{0: types.String("a"), 1: types.String("1")})),
		mustRow(row.New(types.Format_Default, fakeSchema, row.TaggedValues{0: types.String("b"), 1: types.String("2")})),
		mustRow(row.New(types.Format_Default, fakeSchema, row.TaggedValues{0: types.String("c"), 1: types.String("3")})),
	}

	imt = table.NewInMemTableWithData(fakeSchema, imtRows)
}

func TestExists(t *testing.T) {
	testLocations := []DataLocation{
		NewDataLocation(testTableName, ""),
		NewDataLocation("file.csv", ""),
		NewDataLocation("file.psv", ""),
		NewDataLocation("file.json", ""),
		//NewDataLocation("file.nbf", ""),
	}

	_, root, fs := createRootAndFS()

	for _, loc := range testLocations {
		t.Run(loc.String(), func(t *testing.T) {
			if exists, err := loc.Exists(context.Background(), root, fs); err != nil {
				t.Error(err)
			} else if exists {
				t.Error("Shouldn't exist before creation")
			}

			if tableVal, isTable := loc.(TableDataLocation); isTable {
				var err error
				root, err = root.CreateEmptyTable(context.Background(), tableVal.Name, fakeSchema)
				assert.NoError(t, err)
			} else if fileVal, isFile := loc.(FileDataLocation); isFile {
				err := fs.WriteFile(fileVal.Path, []byte("test"))
				assert.NoError(t, err)
			}

			if exists, err := loc.Exists(context.Background(), root, fs); err != nil {
				t.Error(err)
			} else if !exists {
				t.Error("Should already exist after creation")
			}
		})
	}
}

type testDataMoverOptions struct{}

func (t testDataMoverOptions) WritesToTable() bool {
	return true
}

func (t testDataMoverOptions) SrcName() string {
	return ""
}

func (t testDataMoverOptions) DestName() string {
	return testTableName
}

func TestCreateRdWr(t *testing.T) {
	tests := []struct {
		dl          DataLocation
		expectedRdT reflect.Type
		expectedWrT reflect.Type
	}{
		{NewDataLocation("file.csv", ""), reflect.TypeOf((*csv.CSVReader)(nil)).Elem(), reflect.TypeOf((*csv.CSVWriter)(nil)).Elem()},
		{NewDataLocation("file.psv", ""), reflect.TypeOf((*csv.CSVReader)(nil)).Elem(), reflect.TypeOf((*csv.CSVWriter)(nil)).Elem()},
		{NewDataLocation("file.json", ""), reflect.TypeOf((*json.JSONReader)(nil)).Elem(), reflect.TypeOf((*json.JSONWriter)(nil)).Elem()},
		//{NewDataLocation("file.nbf", ""), reflect.TypeOf((*nbf.NBFReader)(nil)).Elem(), reflect.TypeOf((*nbf.NBFWriter)(nil)).Elem()},
	}

	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)
	dEnv.FS.WriteFile(testSchemaFileName, []byte(testSchema))

	mvOpts := &testDataMoverOptions{}

	for idx, test := range tests {
		fmt.Println(idx)

		loc := test.dl

		wr, err := loc.NewCreatingWriter(context.Background(), mvOpts, dEnv, root, true, fakeSchema, nil, true)

		if err != nil {
			t.Fatal("Unexpected error creating writer.", err)
		}

		actualWrT := reflect.TypeOf(wr).Elem()
		if actualWrT != test.expectedWrT {
			t.Fatal("Unexpected writer type. Expected:", test.expectedWrT.Name(), "actual:", actualWrT.Name())
		}

		inMemRd := table.NewInMemTableReader(imt)
		_, numBad, pipeErr := table.PipeRows(context.Background(), inMemRd, wr, false)
		wr.Close(context.Background())

		if numBad != 0 || pipeErr != nil {
			t.Fatal("Failed to write data. bad:", numBad, err)
		}

		if wr, ok := wr.(DataMoverCloser); ok {
			root, err = wr.Flush(context.Background())
			assert.NoError(t, err)
		}

		rd, _, err := loc.NewReader(context.Background(), root, dEnv.FS, JSONOptions{TableName: testTableName, SchFile: testSchemaFileName})

		if err != nil {
			t.Fatal("Unexpected error creating writer", err)
		}

		actualRdT := reflect.TypeOf(rd).Elem()
		if actualRdT != test.expectedRdT {
			t.Error("Unexpected reader type. Expected:", test.expectedRdT.Name(), "actual:", actualRdT.Name())
		}

		rd.Close(context.Background())
	}
}
