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

package sqle

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleReplaceQueryTest = "" //"Natural join with join clause"

// Structure for a test of a replace query
type ReplaceTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The replace query to run
	ReplaceQuery string
	// The select query to run to verify the results
	SelectQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows this query should return, nil if an error is expected
	ExpectedRows []sql.Row
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
}

// BasicReplaceTests cover basic replace statement features and error handling
var BasicReplaceTests = []ReplaceTest{
	{
		Name:           "replace no columns",
		ReplaceQuery:   "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "replace set",
		ReplaceQuery: "replace into people set id = 2, first_name = 'Bart', last_name = 'Simpson'," +
			"is_married = false, age = 10, rating = 9, uuid = '00000000-0000-0000-0000-000000000002', num_episodes = 222",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:         "replace no columns too few values",
		ReplaceQuery: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002')",
		ExpectedErr:  "number of values does not match number of columns provided",
	},
	{
		Name:         "replace no columns too many values",
		ReplaceQuery: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222, 'abc')",
		ExpectedErr:  "number of values does not match number of columns provided",
	},
	{
		Name:           "replace full columns",
		ReplaceQuery:   "replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "replace full columns mixed order",
		ReplaceQuery:   "replace into people (num_episodes, uuid, rating, age, is_married, last_name, first_name, id) values (222, '00000000-0000-0000-0000-000000000002', 9, 10, false, 'Simpson', 'Bart', 2)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "replace full columns negative values",
		ReplaceQuery: `replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values
					    (-7, "Maggie", "Simpson", false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`,
		SelectQuery:    "select * from people where id = -7 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, NewPeopleRowWithOptionalFields(-7, "Maggie", "Simpson", false, -1, -5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "replace full columns null values",
		ReplaceQuery:   "replace into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', null, null, null, null, null)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(CompressSchema(PeopleTestSchema), NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:         "replace partial columns",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson')",
		SelectQuery:  "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:         "replace partial columns mixed order",
		ReplaceQuery: "replace into people (last_name, first_name, id) values ('Simpson', 'Bart', 2)",
		SelectQuery:  "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:         "replace partial columns duplicate column",
		ReplaceQuery: "replace into people (id, first_name, last_name, first_name) values (2, 'Bart', 'Simpson', 'Bart')",
		ExpectedErr:  "column 'first_name' specified twice",
	},
	{
		Name:         "replace partial columns invalid column",
		ReplaceQuery: "replace into people (id, first_name, last_name, middle) values (2, 'Bart', 'Simpson', 'Nani')",
		ExpectedErr:  "Unknown column 'middle' in 'people'",
	},
	{
		Name:         "replace missing non-nullable column",
		ReplaceQuery: "replace into people (id, first_name) values (2, 'Bart')",
		ExpectedErr:  "Field 'last_name' doesn't have a default value",
	},
	{
		Name:         "replace partial columns mismatch too many values",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson', false)",
		ExpectedErr:  "number of values does not match number of columns provided",
	},
	{
		Name:         "replace partial columns mismatch too few values",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (2, 'Bart')",
		ExpectedErr:  "number of values does not match number of columns provided",
	},
	{
		Name:         "replace partial columns functions",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (2, UPPER('Bart'), 'Simpson')",
		SelectQuery:  "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("BART"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:         "replace partial columns multiple rows 2",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', 'Simpson')",
		SelectQuery:  "select id, first_name, last_name from people where id < 2 order by id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(0), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.String("Homer"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "replace partial columns multiple rows 5",
		ReplaceQuery: `replace into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(9, "Jacqueline", "Bouvier", true, 80, 2),
					(10, "Patty", "Bouvier", false, 40, 7),
					(11, "Selma", "Bouvier", false, 40, 7)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id > 6 ORDER BY id",
		ExpectedRows: ToSqlRows(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating"),
			NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
			NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			NewPeopleRow(9, "Jacqueline", "Bouvier", true, 80, 2),
			NewPeopleRow(10, "Patty", "Bouvier", false, 40, 7),
			NewPeopleRow(11, "Selma", "Bouvier", false, 40, 7),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind,
			"is_married", types.IntKind, "age", types.IntKind, "rating", types.FloatKind),
	},
	{
		Name:         "replace partial columns multiple rows null pk",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', null)",
		ExpectedErr:  "column name 'last_name' is non-nullable but attempted to set a value of null",
	},
	{
		Name:         "replace partial columns multiple rows duplicate",
		ReplaceQuery: "replace into people (id, first_name, last_name) values (2, 'Bart', 'Simpson'), (2, 'Bart', 'Simpson')",
		SelectQuery:  "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "replace partial columns multiple rows replace existing pk",
		ReplaceQuery: `replace into people (id, first_name, last_name, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 100)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where rating = 100 order by id",
		ExpectedRows: ToSqlRows(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating"),
			NewPeopleRow(0, "Homer", "Simpson", true, 45, 100),
			NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 100),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind,
			"is_married", types.IntKind, "age", types.IntKind, "rating", types.FloatKind),
	},
	{
		Name: "replace partial columns multiple rows null pk",
		ReplaceQuery: `replace into people (id, first_name, last_name, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(7, "Maggie", null, false, 1, 5.1)`,
		ExpectedErr: "column name 'last_name' is non-nullable but attempted to set a value of null",
	},
	{
		Name: "replace partial columns existing pk",
		AdditionalSetup: ExecuteSetupSQL(context.Background(), `
			CREATE TABLE temppeople (id bigint primary key, first_name varchar(1023), last_name varchar(1023), num bigint);
			INSERT INTO temppeople VALUES (2, 'Bart', 'Simpson', 44);`),
		ReplaceQuery: "replace into temppeople (id, first_name, last_name, num) values (2, 'Bart', 'Simpson', 88)",
		SelectQuery:  "select id, first_name, last_name, num from temppeople where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind, "num", types.IntKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"), types.Int(88))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind, "num", types.IntKind),
	},
}

func TestExecuteReplace(t *testing.T) {
	for _, test := range BasicReplaceTests {
		t.Run(test.Name, func(t *testing.T) {
			testReplaceQuery(t, test)
		})
	}
}

var systemTableReplaceTests = []ReplaceTest{
	{
		Name: "replace into dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs", doltdb.DocsSchema,
			"INSERT INTO dolt_docs VALUES ('LICENSE.md','A license')"),
		ReplaceQuery:   "replace into dolt_docs (doc_name, doc_text) values ('LICENSE.md', 'Some text')",
		SelectQuery:    "select * from dolt_docs",
		ExpectedRows:   []sql.Row{{"LICENSE.md", "Some text"}},
		ExpectedSchema: CompressSchema(doltdb.DocsSchema),
	},
	{
		Name: "replace into dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName, SchemaTableSchema(),
			"CREATE VIEW name as select 2+2 from dual"),
		ReplaceQuery: "replace into dolt_schemas (type, name, fragment) values ('view', 'name', 'create view name as select 1+1 from dual')",
		ExpectedErr:  "table doesn't support INSERT INTO",
	},
}

func TestReplaceIntoSystemTables(t *testing.T) {
	for _, test := range systemTableReplaceTests {
		t.Run(test.Name, func(t *testing.T) {
			testReplaceQuery(t, test)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testReplaceQuery(t *testing.T, test ReplaceTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleReplaceQueryTest) > 0 && test.Name != singleReplaceQueryTest {
		t.Skip("Skipping tests until " + singleReplaceQueryTest)
	}

	ctx := context.Background()
	dEnv, err := CreateEmptyTestDatabase()
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(ctx)
	root, err = executeModify(t, ctx, dEnv, root, test.ReplaceQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		assert.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(t, context.Background(), dEnv, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, unwrapRows(t, test.ExpectedRows), unwrapRows(t, actualRows))
	assertSchemasEqual(t, mustSqlSchema(test.ExpectedSchema), sch)
}
