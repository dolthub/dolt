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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleUpdateQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenUpdate = true

// Structure for a test of an update query
type UpdateTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The update query to run
	UpdateQuery string
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

// BasicUpdateTests cover basic update statement features and error handling
var BasicUpdateTests = []UpdateTest{
	{
		Name:           "update one row, one col, primary key where clause",
		UpdateQuery:    `update people set first_name = "Domer" where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Domer")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update one row, one col, non-primary key where clause",
		UpdateQuery:    `update people set first_name = "Domer" where first_name = "Homer"`,
		SelectQuery:    `select * from people where first_name = "Domer"`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Domer")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update one row, two cols, primary key where clause",
		UpdateQuery:    `update people set first_name = "Ned", last_name = "Flanders" where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Ned", LastNameTag, "Flanders")),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, all cols, non-primary key where clause",
		UpdateQuery: `update people set first_name = "Ned", last_name = "Flanders", is_married = false, rating = 10,
				age = 45, num_episodes = 150, uuid = '00000000-0000-0000-0000-000000000050'
				where age = 38`,
		SelectQuery: `select * from people where uuid = '00000000-0000-0000-0000-000000000050'`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Ned", LastNameTag, "Flanders", IsMarriedTag, false,
				RatingTag, 10.0, AgeTag, 45, NumEpisodesTag, uint64(150),
				UuidTag, uuid.MustParse("00000000-0000-0000-0000-000000000050"))),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, set columns to existing values",
		UpdateQuery: `update people set first_name = "Homer", last_name = "Simpson", is_married = true, rating = 8.5, age = 40,
				num_episodes = null, uuid = null
				where id = 0`,
		SelectQuery:    `select * from people where id = 0`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update one row, null out existing values",
		UpdateQuery: `update people set first_name = "Homer", last_name = "Simpson", is_married = null, rating = null, age = null,
				num_episodes = null, uuid = null
				where first_name = "Homer"`,
		SelectQuery:    `select * from people where first_name = "Homer"`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, MutateRow(PeopleTestSchema, Homer, IsMarriedTag, nil, RatingTag, nil, AgeTag, nil)),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "update multiple rows, set two columns",
		UpdateQuery: `update people set first_name = "Changed", rating = 0.0
				where last_name = "Simpson"`,
		SelectQuery: `select * from people where last_name = "Simpson"`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Changed", RatingTag, 0.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "update no matching rows",
		UpdateQuery:    `update people set first_name = "Changed", rating = 0.0 where last_name = "Flanders"`,
		SelectQuery:    `select * from people`,
		ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update without where clause",
		UpdateQuery: `update people set first_name = "Changed", rating = 0.0`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Moe, FirstNameTag, "Changed", RatingTag, 0.0),
			MutateRow(PeopleTestSchema, Barney, FirstNameTag, "Changed", RatingTag, 0.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update set first_name = last_name",
		UpdateQuery: `update people set first_name = last_name`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, FirstNameTag, "Simpson"),
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Simpson"),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Simpson"),
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Simpson"),
			MutateRow(PeopleTestSchema, Moe, FirstNameTag, "Szyslak"),
			MutateRow(PeopleTestSchema, Barney, FirstNameTag, "Gumble"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update increment age",
		UpdateQuery: `update people set age = age + 1`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, AgeTag, 41),
			MutateRow(PeopleTestSchema, Marge, AgeTag, 39),
			MutateRow(PeopleTestSchema, Bart, AgeTag, 11),
			MutateRow(PeopleTestSchema, Lisa, AgeTag, 9),
			MutateRow(PeopleTestSchema, Moe, AgeTag, 49),
			MutateRow(PeopleTestSchema, Barney, AgeTag, 41),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update reverse rating",
		UpdateQuery: `update people set rating = -rating`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, RatingTag, -8.5),
			MutateRow(PeopleTestSchema, Marge, RatingTag, -8.0),
			MutateRow(PeopleTestSchema, Bart, RatingTag, -9.0),
			MutateRow(PeopleTestSchema, Lisa, RatingTag, -10.0),
			MutateRow(PeopleTestSchema, Moe, RatingTag, -6.5),
			MutateRow(PeopleTestSchema, Barney, RatingTag, -4.0),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update datetime field",
		UpdateQuery: `update episodes set air_date = "1993-03-24 20:00:00" where id = 1`,
		SelectQuery: `select * from episodes where id = 1`,
		ExpectedRows: ToSqlRows(EpisodesTestSchema,
			MutateRow(EpisodesTestSchema, Ep1, EpAirDateTag, DatetimeStrToTimestamp("1993-03-24 20:00:00")),
		),
		ExpectedSchema: CompressSchema(EpisodesTestSchema),
	},
	{
		Name:        "update datetime field",
		UpdateQuery: `update episodes set name = "fake_name" where id = 1;`,
		SelectQuery: `select * from episodes where id = 1;`,
		ExpectedRows: ToSqlRows(EpisodesTestSchema,
			MutateRow(EpisodesTestSchema, Ep1, EpNameTag, "fake_name"),
		),
		ExpectedSchema: CompressSchema(EpisodesTestSchema),
	},
	{
		Name:        "update multiple rows, =",
		UpdateQuery: `update people set first_name = "Homer" where last_name = "Simpson"`,
		SelectQuery: `select * from people where last_name = "Simpson"`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <>",
		UpdateQuery: `update people set last_name = "Simpson" where last_name <> "Simpson"`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			Marge,
			Bart,
			Lisa,
			MutateRow(PeopleTestSchema, Moe, LastNameTag, "Simpson"),
			MutateRow(PeopleTestSchema, Barney, LastNameTag, "Simpson"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, >",
		UpdateQuery: `update people set first_name = "Homer" where age > 10`,
		SelectQuery: `select * from people where age > 10`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Moe, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Barney, FirstNameTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, >=",
		UpdateQuery: `update people set first_name = "Homer" where age >= 10`,
		SelectQuery: `select * from people where age >= 10`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Moe, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Barney, FirstNameTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <",
		UpdateQuery: `update people set first_name = "Bart" where age < 40`,
		SelectQuery: `select * from people where age < 40`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Bart"),
			Bart,
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Bart"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows, <=",
		UpdateQuery: `update people set first_name = "Homer" where age <= 40`,
		SelectQuery: `select * from people where age <= 40`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			Homer,
			MutateRow(PeopleTestSchema, Marge, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Bart, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Lisa, FirstNameTag, "Homer"),
			MutateRow(PeopleTestSchema, Barney, FirstNameTag, "Homer"),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows pk increment order by desc",
		UpdateQuery: `update people set id = id + 1 order by id desc`,
		SelectQuery: `select * from people`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, IdTag, HomerId+1),
			MutateRow(PeopleTestSchema, Marge, IdTag, MargeId+1),
			MutateRow(PeopleTestSchema, Bart, IdTag, BartId+1),
			MutateRow(PeopleTestSchema, Lisa, IdTag, LisaId+1),
			MutateRow(PeopleTestSchema, Moe, IdTag, MoeId+1),
			MutateRow(PeopleTestSchema, Barney, IdTag, BarneyId+1),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows pk increment order by desc",
		UpdateQuery: `update people set id = id + 1 order by id desc`,
		SelectQuery: `select * from people order by id`,
		ExpectedRows: ToSqlRows(PeopleTestSchema,
			MutateRow(PeopleTestSchema, Homer, IdTag, HomerId+1),
			MutateRow(PeopleTestSchema, Marge, IdTag, MargeId+1),
			MutateRow(PeopleTestSchema, Bart, IdTag, BartId+1),
			MutateRow(PeopleTestSchema, Lisa, IdTag, LisaId+1),
			MutateRow(PeopleTestSchema, Moe, IdTag, MoeId+1),
			MutateRow(PeopleTestSchema, Barney, IdTag, BarneyId+1),
		),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:        "update multiple rows pk increment order by asc",
		UpdateQuery: `update people set id = id + 1 order by id asc`,
		ExpectedErr: "duplicate primary key",
	},
	{
		Name:        "update primary key col",
		UpdateQuery: `update people set id = 0 where first_name = "Marge"`,
		ExpectedErr: "duplicate primary key",
	},
	{
		Name:        "null constraint failure",
		UpdateQuery: `update people set first_name = null where id = 0`,
		ExpectedErr: "Constraint failed for column 'first_name': Not null",
	},
	{
		Name:        "type mismatch list -> string",
		UpdateQuery: `update people set first_name = ("one", "two") where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> int",
		UpdateQuery: `update people set age = "pretty old" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> float",
		UpdateQuery: `update people set rating = "great" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name:        "type mismatch string -> uint",
		UpdateQuery: `update people set num_episodes = "all of them" where id = 0`,
		ExpectedErr: "Type mismatch",
	},
}

func TestExecuteUpdate(t *testing.T) {
	for _, test := range BasicUpdateTests {
		t.Run(test.Name, func(t *testing.T) {
			testUpdateQuery(t, test)
		})
	}
}

func TestExecuteUpdateSystemTables(t *testing.T) {
	for _, test := range systemTableUpdateTests {
		t.Run(test.Name, func(t *testing.T) {
			testUpdateQuery(t, test)
		})
	}
}

var systemTableUpdateTests = []UpdateTest{
	{
		Name: "update dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs", doltdb.DocsSchema,
			"INSERT INTO dolt_docs VALUES ('LICENSE.md','A license')"),
		UpdateQuery:    "update dolt_docs set doc_text = 'Some text';",
		SelectQuery:    "select * from dolt_docs",
		ExpectedRows:   []sql.Row{{"LICENSE.md", "Some text"}},
		ExpectedSchema: CompressSchema(doltdb.DocsSchema),
	},
	{
		Name: "update dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName, dtables.DoltQueryCatalogSchema,
			"INSERT INTO dolt_query_catalog VALUES ('abc123', 1, 'example', 'select 2+2 from dual', 'description')"),
		UpdateQuery:    "update dolt_query_catalog set display_order = display_order + 1",
		SelectQuery:    "select * from dolt_query_catalog",
		ExpectedRows:   []sql.Row{{"abc123", uint64(2), "example", "select 2+2 from dual", "description"}},
		ExpectedSchema: CompressSchema(dtables.DoltQueryCatalogSchema),
	},
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testUpdateQuery(t *testing.T, test UpdateTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleUpdateQueryTest) > 0 && test.Name != singleUpdateQueryTest {
		t.Skip("Skipping tests until " + singleUpdateQueryTest)
	}

	ctx := context.Background()
	dEnv, err := CreateTestDatabase()
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(ctx)
	root, err = executeModify(t, ctx, dEnv, root, test.UpdateQuery)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	actualRows, sch, err := executeSelect(t, ctx, dEnv, root, test.SelectQuery)
	require.NoError(t, err)

	actualRows = unwrapRows(t, actualRows)
	expectedRows := unwrapRows(t, test.ExpectedRows)

	assert.Equal(t, len(expectedRows), len(actualRows))
	for i := 0; i < len(expectedRows); i++ {
		assert.Equal(t, len(expectedRows[i]), len(actualRows[i]))
		for j := 0; j < len(expectedRows[i]); j++ {
			if _, ok := actualRows[i][j].(json.NomsJSON); ok {
				cmp, err := gmstypes.CompareJSON(ctx, actualRows[i][j].(json.NomsJSON), expectedRows[i][j].(json.NomsJSON))
				assert.NoError(t, err)
				assert.Equal(t, 0, cmp)
			} else {
				assert.Equal(t, expectedRows[i][j], actualRows[i][j])
			}
		}
	}

	sqlSchema := mustSqlSchema(test.ExpectedSchema)
	assertSchemasEqual(t, sqlSchema, sch)
}
