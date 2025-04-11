// Copyright 2020 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleInsertQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenInsert = true

// Structure for a test of a insert query
type InsertTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The insert query to run
	InsertQuery string
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
	// Whether to skip this test on SqlEngine (go-mysql-server) execution.
	// Over time, this should become false for every query.
	SkipOnSqlEngine bool
}

// BasicInsertTests cover basic insert statement features and error handling
var BasicInsertTests = []InsertTest{
	{
		Name:           "insert no columns",
		InsertQuery:    "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "insert no columns too few values",
		InsertQuery: "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002')",
		ExpectedErr: "number of values does not match number of columns provided",
	},
	{
		Name:        "insert no columns too many values",
		InsertQuery: "insert into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222, 'abc')",
		ExpectedErr: "number of values does not match number of columns provided",
	},
	{
		Name:           "insert full columns",
		InsertQuery:    "insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "insert full columns mixed order",
		InsertQuery:    "insert into people (num_episodes, uuid, rating, age, is_married, last_name, first_name, id) values (222, '00000000-0000-0000-0000-000000000002', 9, 10, false, 'Simpson', 'Bart', 2)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "insert full columns negative values",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values
					    (-7, "Maggie", "Simpson", false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`,
		SelectQuery:    "select * from people where id = -7 ORDER BY id",
		ExpectedRows:   ToSqlRows(PeopleTestSchema, NewPeopleRowWithOptionalFields(-7, "Maggie", "Simpson", false, -1, -5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "insert full columns null values",
		InsertQuery:    "insert into people (id, first_name, last_name, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', null, null, null, null, null)",
		SelectQuery:    "select * from people where id = 2 ORDER BY id",
		ExpectedRows:   ToSqlRows(CompressSchema(PeopleTestSchema), NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "insert partial columns",
		InsertQuery: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson')",
		SelectQuery: "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:        "insert partial columns mixed order",
		InsertQuery: "insert into people (last_name, first_name, id) values ('Simpson', 'Bart', 2)",
		SelectQuery: "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:        "insert partial columns duplicate column",
		InsertQuery: "insert into people (id, first_name, last_name, first_name) values (2, 'Bart', 'Simpson', 'Bart')",
		ExpectedErr: "column 'first_name' specified twice",
	},
	{
		Name:        "insert partial columns invalid column",
		InsertQuery: "insert into people (id, first_name, last_name, middle) values (2, 'Bart', 'Simpson', 'Nani')",
		ExpectedErr: "Unknown column 'middle' in 'people'",
	},
	{
		Name:        "insert missing non-nullable column",
		InsertQuery: "insert into people (id, first_name) values (2, 'Bart')",
		ExpectedErr: "Field 'last_name' doesn't have a default value",
	},
	{
		Name:        "insert partial columns mismatch too many values",
		InsertQuery: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson', false)",
		ExpectedErr: "number of values does not match number of columns provided",
	},
	{
		Name:        "insert partial columns mismatch too few values",
		InsertQuery: "insert into people (id, first_name, last_name) values (2, 'Bart')",
		ExpectedErr: "number of values does not match number of columns provided",
	},
	{
		Name:        "insert partial columns functions",
		InsertQuery: "insert into people (id, first_name, last_name) values (2, UPPER('Bart'), 'Simpson')",
		SelectQuery: "select id, first_name, last_name from people where id = 2 ORDER BY id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.String("BART"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:        "insert partial columns multiple rows 2",
		InsertQuery: "insert into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', 'Simpson')",
		SelectQuery: "select id, first_name, last_name from people where id < 2 order by id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(0), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.String("Homer"), types.String("Simpson")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "insert partial columns multiple rows 5",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
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
		Name: "insert ignore partial columns multiple rows null constraint failure",
		InsertQuery: `insert ignore into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", null, false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
		SelectQuery:  "select id, first_name, last_name, is_married, age, rating from people where id > 6 ORDER BY id",
		ExpectedRows: ToSqlRows(PeopleTestSchema, NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5)),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind,
			"is_married", types.IntKind, "age", types.IntKind, "rating", types.FloatKind),
		SkipOnSqlEngine: true,
	},
	{
		Name: "insert ignore partial columns multiple rows existing pk",
		InsertQuery: `insert ignore into people (id, first_name, last_name, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 8.5)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where rating = 8.5 order by id",
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 8.5),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind,
			"is_married", types.IntKind, "age", types.IntKind, "rating", types.FloatKind),
		SkipOnSqlEngine: true,
	},
	{
		Name: "insert ignore partial columns multiple rows duplicate pk",
		InsertQuery: `insert ignore into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
		SelectQuery:  "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(PeopleTestSchema, NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1)),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first_name", types.StringKind, "last_name", types.StringKind,
			"is_married", types.IntKind, "age", types.IntKind, "rating", types.FloatKind),
		SkipOnSqlEngine: true,
	},
	{
		Name:        "insert partial columns multiple rows null pk",
		InsertQuery: "insert into people (id, first_name, last_name) values (0, 'Bart', 'Simpson'), (1, 'Homer', null)",
		ExpectedErr: "column name 'last_name' is non-nullable but attempted to set a value of null",
	},
	{
		Name:        "insert partial columns multiple rows duplicate",
		InsertQuery: "insert into people (id, first_name, last_name) values (2, 'Bart', 'Simpson'), (2, 'Bart', 'Simpson')",
		ExpectedErr: "duplicate primary key",
	},
	{
		Name: "insert partial columns existing pk",
		AdditionalSetup: ExecuteSetupSQL(context.Background(), `
			CREATE TABLE temppeople (id bigint primary key, first_name varchar(1023), last_name varchar(1023));
			INSERT INTO temppeople VALUES (2, 'Bart', 'Simpson');`),
		InsertQuery: "insert into temppeople (id, first_name, last_name) values (2, 'Bart', 'Simpson')",
		ExpectedErr: "duplicate primary key",
	},
	{
		Name: "type mismatch int -> string",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", 100, false, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("100"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch int -> bool",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", 1, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(1), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch string -> int",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					("7", "Maggie", "Simpson", false, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch string -> float",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, "5.1")`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch string -> uint",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, num_episodes) values
					(7, "Maggie", "Simpson", false, 1, "100")`,
		SelectQuery: "select id, first_name, last_name, is_married, age, num_episodes from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "num_episodes")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Uint(100)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "num_episodes")),
	},
	{
		Name: "type mismatch float -> string",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, 8.1, "Simpson", false, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("8.1"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch float -> bool",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", 0.5, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(1), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch float -> int",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1.0, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch bool -> int",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(true, "Maggie", "Simpson", false, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 1 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(1), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch bool -> float",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, true)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("Maggie"), types.String("Simpson"), types.Int(0), types.Int(1), types.Float(1.0)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
	{
		Name: "type mismatch bool -> string",
		InsertQuery: `insert into people (id, first_name, last_name, is_married, age, rating) values
					(7, true, "Simpson", false, 1, 5.1)`,
		SelectQuery: "select id, first_name, last_name, is_married, age, rating from people where id = 7 ORDER BY id",
		ExpectedRows: ToSqlRows(
			CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
			NewResultSetRow(types.Int(7), types.String("1"), types.String("Simpson" /*"West"*/), types.Int(0), types.Int(1), types.Float(5.1)),
		),
		ExpectedSchema: CompressSchema(SubsetSchema(PeopleTestSchema, "id", "first_name", "last_name", "is_married", "age", "rating")),
	},
}

func TestExecuteInsert(t *testing.T) {
	for _, test := range BasicInsertTests {
		t.Run(test.Name, func(t *testing.T) {
			testInsertQuery(t, test)
		})
	}
}

var systemTableInsertTests = []InsertTest{
	{
		Name:            "insert into dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs", doltdb.DocsSchema, ""),
		InsertQuery:     "insert into dolt_docs (doc_name, doc_text) values ('README.md', 'Some text')",
		SelectQuery:     "select * from dolt_docs",
		ExpectedRows:    []sql.Row{{"README.md", "Some text"}},
		ExpectedSchema:  CompressSchema(doltdb.DocsSchema),
	},
	{
		Name: "insert into dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName, dtables.DoltQueryCatalogSchema,
			"INSERT INTO dolt_query_catalog VALUES ('existingEntry', 2, 'example', 'select 2+2 from dual', 'description')"),
		InsertQuery: "insert into dolt_query_catalog (id, display_order, name, query, description) values ('abc123', 1, 'example', 'select 1+1 from dual', 'description')",
		SelectQuery: "select * from dolt_query_catalog ORDER BY id",
		ExpectedRows: ToSqlRows(CompressSchema(dtables.DoltQueryCatalogSchema),
			NewRow(types.String("abc123"), types.Uint(1), types.String("example"), types.String("select 1+1 from dual"), types.String("description")),
			NewRow(types.String("existingEntry"), types.Uint(2), types.String("example"), types.String("select 2+2 from dual"), types.String("description")),
		),
		ExpectedSchema: CompressSchema(dtables.DoltQueryCatalogSchema),
	},
	{
		Name:            "insert into dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName, SchemaTableSchema(), ""),
		InsertQuery:     "insert into dolt_schemas (type, name, fragment) values ('view', 'name', 'create view name as select 2+2 from dual')",
		ExpectedErr:     "table doesn't support INSERT INTO",
	},
}

func TestInsertIntoSystemTables(t *testing.T) {
	for _, test := range systemTableInsertTests {
		t.Run(test.Name, func(t *testing.T) {
			testInsertQuery(t, test)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testInsertQuery(t *testing.T, test InsertTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleInsertQueryTest) > 0 && test.Name != singleInsertQueryTest {
		t.Skip("Skipping tests until " + singleInsertQueryTest)
	}

	if len(singleInsertQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenInsert {
		t.Skip("Skipping test broken on SQL engine")
	}

	ctx := context.Background()
	dEnv, err := CreateEmptyTestDatabase()
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(ctx)
	root, err = executeModify(t, ctx, dEnv, root, test.InsertQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		assert.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(t, ctx, dEnv, root, test.SelectQuery)
	require.NoError(t, err)

	assert.Equal(t, test.ExpectedRows, actualRows)
	assertSchemasEqual(t, mustSqlSchema(test.ExpectedSchema), sch)
}
