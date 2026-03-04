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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
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

func createRootAndFS() (*doltdb.DoltDB, doltdb.RootValue, filesys.Filesys) {

	testHomeDir := "/user/bheni"
	workingDir := "/user/bheni/datasets/states"
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	fs.WriteFile(testSchemaFileName, []byte(testSchema), os.ModePerm)
	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.InMemDoltDB, filesys.LocalFS)
	ddb.WriteEmptyRepo(context.Background(), "master", "billy bob", "bigbillieb@fake.horse")

	cs, _ := doltdb.NewCommitSpec("master")
	optCmt, _ := ddb.Resolve(context.Background(), cs, nil)
	commit, _ := optCmt.ToCommit()
	root, err := commit.GetRootValue(context.Background())

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
		{NewDataLocation("file.csv", ""), CsvFile.ReadableStr() + ":file.csv", true},
		{NewDataLocation("file.psv", ""), PsvFile.ReadableStr() + ":file.psv", true},
		{NewDataLocation("file.json", ""), JsonFile.ReadableStr() + ":file.json", true},
		{NewDataLocation("file.jsonl", ""), JsonlFile.ReadableStr() + ":file.jsonl", true},
		{NewDataLocation("file.ignored", "jsonl"), JsonlFile.ReadableStr() + ":file.ignored", true},
		// {NewDataLocation("file.nbf", ""), NbfFile, "file.nbf", true},
	}

	for _, test := range tests {
		t.Run(test.dl.String(), func(t *testing.T) {
			assert.Equal(t, test.expectedStr, test.dl.String())

			_, isFileType := test.dl.(FileDataLocation)
			assert.Equal(t, test.expectedIsFileType, isFileType)
		})
	}
}

func TestExists(t *testing.T) {
	testLocations := []DataLocation{
		NewDataLocation("file.csv", ""),
		NewDataLocation("file.psv", ""),
		NewDataLocation("file.json", ""),
		NewDataLocation("file.jsonl", ""),
		// NewDataLocation("file.nbf", ""),
	}

	ddb, root, fs := createRootAndFS()
	defer ddb.Close()

	for _, loc := range testLocations {
		t.Run(loc.String(), func(t *testing.T) {
			if exists, err := loc.Exists(context.Background(), root, fs); err != nil {
				t.Error(err)
			} else if exists {
				t.Error("Shouldn't exist before creation")
			}

			if fileVal, isFile := loc.(FileDataLocation); isFile {
				err := fs.WriteFile(fileVal.Path, []byte("test"), os.ModePerm)
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
