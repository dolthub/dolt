package mvdata

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"testing"
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
		},
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
	"fields": [
		{"name": "key", "kind": "string", "required": true},
		{"name": "value", "kind": "int", "required": true}
	],
	"constraints": [
		{"constraint_type":"primary_key", "field_indices":[0]}
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
		var verr errhand.VerboseError
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

		seedWr, verr := src.CreateOverwritingDataWriter(root, fs, true, fakeSchema)

		if verr != nil {
			t.Fatal(verr.Verbose())
		}

		imtRd := table.NewInMemTableReader(imt)

		_, _, err = table.PipeRows(imtRd, seedWr, false)
		seedWr.Close()
		imtRd.Close()

		if err != nil {
			t.Fatal(err)
		}

		dm, verr := NewDataMover(root, fs, test.mvOpts)

		if verr != nil {
			t.Fatal(verr.Verbose())
		}

		err = dm.Move()

		if err != nil {
			t.Fatal(err)
		}
	}
}
