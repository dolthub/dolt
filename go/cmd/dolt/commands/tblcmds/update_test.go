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

package tblcmds

/*
func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(rd, wr, false)
	rd.Close()
	wr.Close()

	if err != nil {
		t.Error("Failed to seed initial data", err)
	}

	err = dEnv.PutTableToWorking(*wr.GetMap(), wr.GetSchema(), tableName)

	if err != nil {
		t.Error("Unable to put initial value of table in in mem noms db", err)
	}

	return dEnv
}

var expectedRows []*table.Row
var expectedIMT *table.InMemTable

func init() {
	uuids := []types.UUID{
		types.UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000"))),
		types.UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001"))),
		types.UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))),
		types.UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000003"))),
	}

	sch := dtestutils.TypedSchema
	expectedRows = []*table.Row{
		table.NewRow(table.RowDataFromValues(sch, []types.Value{uuids[0], types.String("Aaron Aaronson"), types.Uint(55), types.String("the best"), types.Bool(true)})),
		table.NewRow(table.RowDataFromValues(sch, []types.Value{uuids[1], types.String("John Johnson"), types.Uint(25), types.String("Dufus"), types.Bool(false)})),
		table.NewRow(table.RowDataFromValues(sch, []types.Value{uuids[2], types.String("Rob Robertson"), types.Uint(21), types.String(""), types.Bool(false)})),
		table.NewRow(table.RowDataFromValues(sch, []types.Value{uuids[3], types.String("Morris Morrison"), types.Uint(14), types.String(""), types.Bool(false)})),
	}

	expectedIMT = table.NewInMemTableWithData(dtestutils.TypedSchema, expectedRows)
}

func TestUpdate(t *testing.T) {
	tests := []struct {
		args             []string
		csvData          string
		mappingJSON      string
		expectedExitCode int
		expectedIMT      *table.InMemTable
	}{
		{
			[]string{"-table", tableName, csvPath},
			`id, name, title, age, is_married
00000000-0000-0000-0000-000000000000,Aaron Aaronson,the best,55,true
00000000-0000-0000-0000-000000000003,Morris Morrison,,14,false`,
			"",
			0,
			expectedIMT,
		},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)

		err := dEnv.FS.WriteFile(csvPath, []byte(test.csvData))

		if err != nil {
			t.Fatal("Failed to create mapping file.")
		}

		if test.mappingJSON != "" {
			err = dEnv.FS.WriteFile(mappingPath, []byte(test.mappingJSON))

			if err != nil {
				t.Fatal("Failed to create mapping file.")
			}
		}

		exitCode := Import("dolt edit create", test.args, dEnv)

		if exitCode != test.expectedExitCode {
			commandLine := "dolt edit update " + strings.Join(test.args, " ")
			t.Error(commandLine, "returned with exit code", exitCode, "expected", test.expectedExitCode)
		}

		dtestutils.CheckResultTable(t, tableName, dEnv, test.expectedIMT, "id")
	}
}

func TestParseUpdateArgs(t *testing.T) {
	tests := []struct {
		args         []string
		expectedOpts *mvdata.MoveOptions
	}{
		{[]string{}, nil},
		{[]string{"-table", "table_name"}, nil},
		{
			[]string{"-table", "table_name", "file.csv"},
			&mvdata.MoveOptions{
				mvdata.UpdateOp,
				false,
				"",
				"",
				"",
				&mvdata.DataLocation{Path: "file.csv", Format: mvdata.CsvFile},
				&mvdata.DataLocation{Path: "table_name", Format: mvdata.DoltDB},
			},
		},
		{
			[]string{"-table", "table_name", "file.unsupported"},
			nil,
		},
		{
			[]string{"-table", "invalid_table_name.csv", "file.csv"},
			nil,
		},
		{
			[]string{"-table", "table_name", "-map", "mapping.json", "-continue", "file.nbf"},
			&mvdata.MoveOptions{
				mvdata.UpdateOp,
				true,
				"",
				"mapping.json",
				"",
				&mvdata.DataLocation{Path: "file.nbf", Format: mvdata.NbfFile},
				&mvdata.DataLocation{Path: "table_name", Format: mvdata.DoltDB},
			},
		},
	}

	for _, test := range tests {
		actualOpts := parseUpdateArgs("dolt edit update", test.args)

		if !optsEqual(test.expectedOpts, actualOpts) {
			argStr := strings.Join(test.args, " ")
			t.Error("Unexpected result for args:", argStr)
		}
	}
}*/
