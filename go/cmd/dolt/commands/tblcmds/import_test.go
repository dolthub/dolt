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
const (
	tableName = "result_table"

	csvPath     = "/user/bheni/file.csv"
	psvPath     = "/user/bheni/file.psv"
	nbfPath     = "/user/bheni/file.nbf"
	schemaPath  = "/user/bheni/schema.json"
	mappingPath = "/user/bheni/mapping.json"
)

var fieldNames = []string{"state", "population", "is_state"}

type stateData struct {
	name       string
	population uint64
	isState    bool
}

func (sd stateData) delimSepVal(delim rune) string {
	return fmt.Sprintf("%[2]s%[1]c%[3]d%[1]c%[4]t", delim, sd.name, sd.population, sd.isState)
}

type stateCollection []stateData

func (sc stateCollection) delimSepVals(delim rune) string {
	stateStrs := make([]string, len(sc)+1)

	stateStrs[0] = strings.Join(fieldNames, string(delim))

	for i, state := range sc {
		stateStrs[i+1] = state.delimSepVal(delim)
	}

	return strings.Join(stateStrs, "\n")
}

var sd = stateCollection{
	{"West Virginia", 176924, false},
	{"South Carolina", 581185, true},
	{"New Hampshire", 269328, true},
	{"Wisconsin", 3635, false},
	{"Vermont", 280652, true},
	{"Georgia", 516823, true},
	{"Pennsylvania", 1348233, true},
	{"Florida", 34730, false},
	{"Kentucky", 687917, true},
	{"Missouri", 140455, true},
}

var csvData = sd.delimSepVals(',')
var psvData = sd.delimSepVals('|')

var untypedSchema = untyped.NewUntypedSchema(fieldNames...)
var untypedRows = make([]*table.Row, len(sd))

func init() {
	for i, curr := range sd {
		popStr := strconv.FormatUint(curr.population, 10)
		isStateStr := map[bool]string{true: "true", false: "false"}[curr.isState]

		untypedRows[i] = table.NewRow(table.RowDataFromValues(untypedSchema, []types.Value{
			types.String(curr.name), types.String(popStr), types.String(isStateStr),
		}))
	}
}

var typedSchemaJson []byte
var typedRows = make([]*table.Row, len(sd))
var typedSchema = schema.NewSchema([]*schema.Field{
	schema.NewField("state", types.StringKind, true),
	schema.NewField("population", types.UintKind, true),
})

func init() {
	typedSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
	typedSchemaJson, _ = jsonenc.SchemaToJSON(typedSchema)

	for i, curr := range sd {
		typedRows[i] = table.NewRow(table.RowDataFromValues(typedSchema, []types.Value{
			types.String(curr.name), types.Uint(curr.population),
		}))
	}
}

var mappedSchemaJson []byte
var mappedRows = make([]*table.Row, len(sd))
var mappedSchema = schema.NewSchema([]*schema.Field{
	schema.NewField("state", types.BoolKind, true),
	schema.NewField("pop", types.UintKind, true),
	schema.NewField("stname", types.StringKind, true),
})
var mappingJson = `{
	"state":"stname",
	"population":"pop",
	"is_state": "state"
}`

func init() {
	mappedSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{2}))
	mappedSchemaJson, _ = jsonenc.SchemaToJSON(mappedSchema)

	for i, curr := range sd {
		mappedRows[i] = table.NewRow(table.RowDataFromValues(mappedSchema, []types.Value{
			types.Bool(curr.isState), types.Uint(curr.population), types.String(curr.name),
		}))
	}
}

type createTest struct {
	args              []string
	expectedExitCode  int
	expectedTable     *table.InMemTable
	pkInExpectedTable string
	inFilePath        string
	inFileContents    string
	schemaJson        []byte
	mappingJson       string
}

func getTests() []createTest {
	return []createTest{
		{
			[]string{"-table", tableName, "-pk", "state", csvPath},
			0,
			table.NewInMemTableWithData(untypedSchema, untypedRows),
			"state",
			csvPath,
			csvData,
			nil,
			"",
		},
		{
			[]string{"-table", tableName, "-schema", schemaPath, psvPath},
			0,
			table.NewInMemTableWithData(typedSchema, typedRows),
			"state",
			psvPath,
			psvData,
			typedSchemaJson,
			"",
		},
		{
			[]string{"-table", tableName, "-schema", schemaPath, "-map", mappingPath, csvPath},
			0,
			table.NewInMemTableWithData(mappedSchema, mappedRows),
			"stname",
			csvPath,
			csvData,
			mappedSchemaJson,
			mappingJson,
		},
	}
}


func initTestEnv(t *testing.T, test *createTest) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	err := dEnv.FS.WriteFile(test.inFilePath, []byte(test.inFileContents))

	if err != nil {
		t.Fatal("Failed to create test csv file.")
	}

	if len(test.schemaJson) > 0 {
		err = dEnv.FS.WriteFile(schemaPath, test.schemaJson)

		if err != nil {
			t.Fatal("Failed to create schema file.")
		}
	}

	if test.mappingJson != "" {
		err = dEnv.FS.WriteFile(mappingPath, []byte(test.mappingJson))

		if err != nil {
			t.Fatal("Failed to create mapping file.")
		}
	}
	return dEnv
}

func TestForceFlag(t *testing.T) {
	test := getTests()[0]
	if test.expectedExitCode != 0 {
		t.Fatal("This only works if the test we are running is expected to succeed.")
	}

	paramSet := set.NewStrSet(test.args)
	if paramSet.Contains("-force") {
		t.Fatal("This only works if the test isn't already using the Force flag.")
	}

	dEnv := initTestEnv(t, &test)

	exitCode := Import("dolt edit create", test.args, dEnv)

	if exitCode != 0 {
		t.Fatal("Initial execution should succeed")
	}

	exitCode = Import("dolt edit create", test.args, dEnv)

	if exitCode == 0 {
		t.Fatal("Second execution should fail without the Force flag")
	}

	forcedArgs := make([]string, len(test.args)+1)
	copy(forcedArgs, test.args)
	forcedArgs[len(test.args)-1], forcedArgs[len(test.args)] = "-force", forcedArgs[len(test.args)-1]

	exitCode = Import("dolt edit create", forcedArgs, dEnv)

	if exitCode != 0 {
		t.Fatal("Third execution should succeed with the Force flag")
	}
}

func TestParseCreateArgs(t *testing.T) {
	tests := []struct {
		args         []string
		expectedOpts *mvdata.MoveOptions
	}{
		{[]string{}, nil},
		{[]string{"-table", "table_name"}, nil},
		{
			[]string{"-table", "table_name", "file.csv"},
			&mvdata.MoveOptions{
				mvdata.OverwriteOp,
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
			[]string{"-table", "table_name", "-schema", "schema.json", "-pk", "id", "-map", "mapping.json", "-continue", "file.nbf"},
			&mvdata.MoveOptions{
				mvdata.OverwriteOp,
				true,
				"schema.json",
				"mapping.json",
				"id",
				&mvdata.DataLocation{Path: "file.nbf", Format: mvdata.NbfFile},
				&mvdata.DataLocation{Path: "table_name", Format: mvdata.DoltDB},
			},
		},
	}

	for _, test := range tests {
		_, actualOpts := parseCreateArgs("dolt edit create", test.args)

		if !optsEqual(test.expectedOpts, actualOpts) {
			argStr := strings.Join(test.args, " ")
			t.Error("Unexpected result for args:", argStr)
		}
	}
}

func optsEqual(opts1, opts2 *mvdata.MoveOptions) bool {
	if opts1 == nil && opts2 == nil {
		return true
	} else if opts1 == nil || opts2 == nil {
		return false
	}

	return reflect.DeepEqual(opts1, opts2)
}


func TestImportCommand(t *testing.T) {
//	tests := getTests()
//	for _, test := range tests {
//		dEnv := initTestEnv(t, &test)
//
//		exitCode := Import("dolt edit create", test.args, dEnv)
//
//		if exitCode != test.expectedExitCode {
//			commandLine := "dolt edit create " + strings.Join(test.args, " ")
//			t.Error(commandLine, "returned with exit code", exitCode, "expected", test.expectedExitCode)
//		}
//
//		dtestutils.CheckResultTable(t, tableName, dEnv, test.expectedTable, test.pkInExpectedTable)
//	}
}*/
