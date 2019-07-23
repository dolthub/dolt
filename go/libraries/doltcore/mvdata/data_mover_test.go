// Copyright 2019 Liquidata, Inc.
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
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
)

const (
	schemaFile  = "schema.json"
	mappingFile = "mapping.json"
)

func TestDataMover(t *testing.T) {
	tests := []struct {
		schemaJSON  string
		mappingJSON string
		mvOpts      *MoveOptions
	}{
		{
			"",
			"",
			&MoveOptions{
				Operation:   OverwriteOp,
				ContOnErr:   false,
				SchFile:     "",
				MappingFile: "",
				PrimaryKey:  "",
				Src:         NewDataLocation("data.csv", ""),
				Dest:        NewDataLocation("data.psv", "psv")},
		},
		/*{
			"",
			"",
			&MoveOptions{
				Operation:   OverwriteOp,
				ContOnErr:   false,
				SchFile:     "",
				MappingFile: "",
				PrimaryKey:  "a",
				Src:         NewDataLocation("data.csv", ""),
				Dest:        NewDataLocation("data.nbf", "")},
		},
		{
			"",
			"",
			&MoveOptions{
				Operation:   OverwriteOp,
				ContOnErr:   false,
				SchFile:     "",
				MappingFile: "",
				PrimaryKey:  "",
				Src:         NewDataLocation("data.nbf", "nbf"),
				Dest:        NewDataLocation("table-name", "")},
		},*/
		{
			"",
			"",
			&MoveOptions{
				Operation:   OverwriteOp,
				ContOnErr:   false,
				SchFile:     "",
				MappingFile: "",
				PrimaryKey:  "a",
				Src:         NewDataLocation("data.csv", ""),
				Dest:        NewDataLocation("table-name", "")},
		},
		{
			`{
	"columns": [
		{
			"name": "key", 
			"kind": "string", 
			"tag": 0, 
			"is_part_of_pk": true, 
			"col_constraints":[
				{
					"constraint_type": "not_null"
				}
			]
		},
		{"name": "value", "kind": "int", "tag": 1}
	]
}`,
			`{"a":"key","b":"value"}`,
			&MoveOptions{
				Operation:   OverwriteOp,
				ContOnErr:   false,
				SchFile:     "",
				MappingFile: "",
				PrimaryKey:  "",
				Src:         NewDataLocation("data.csv", ""),
				Dest:        NewDataLocation("table-name", "")},
		},
	}

	for _, test := range tests {
		var err error
		_, root, fs := createRootAndFS()

		if test.schemaJSON != "" {
			test.mvOpts.SchFile = schemaFile
			err = fs.WriteFile(schemaFile, []byte(test.schemaJSON))
		}

		if test.mappingJSON != "" {
			test.mvOpts.MappingFile = mappingFile
			err = fs.WriteFile(mappingFile, []byte(test.mappingJSON))
		}

		src := test.mvOpts.Src

		seedWr, err := src.CreateOverwritingDataWriter(context.Background(), test.mvOpts, root, fs, true, fakeSchema, nil)

		if err != nil {
			t.Fatal(err.Error())
		}

		imtRd := table.NewInMemTableReader(imt)

		_, _, err = table.PipeRows(context.Background(), imtRd, seedWr, false)
		seedWr.Close(context.Background())
		imtRd.Close(context.Background())

		if err != nil {
			t.Fatal(err)
		}

		encoding.UnmarshalJson(test.schemaJSON)

		dm, crDMErr := NewDataMover(context.Background(), root, fs, test.mvOpts, nil)

		if crDMErr != nil {
			t.Fatal(crDMErr.String())
		}

		err = dm.Move(context.Background())

		if err != nil {
			t.Fatal(err)
		}
	}
}
