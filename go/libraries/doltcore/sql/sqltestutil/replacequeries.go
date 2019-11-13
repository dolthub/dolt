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

package sqltestutil

import (
	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

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
	ExpectedRows []row.Row
	// An expected error string
	ExpectedErr string
	// Setup logic to run before executing this test, after initial tables have been created and populated
	AdditionalSetup SetupFn
	// Whether to skip this test on SqlEngine (go-mysql-server) execution.
	// Over time, this should become false for every query.
	SkipOnSqlEngine bool
}

// BasicReplaceTests cover basic replace statement features and error handling
var BasicReplaceTests = []ReplaceTest{
	{
		Name:           "replace no columns",
		ReplaceQuery:   "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "replace set",
		ReplaceQuery: "replace into people set id = 2, first = 'Bart', last = 'Simpson'," +
			"is_married = false, age = 10, rating = 9, uuid = '00000000-0000-0000-0000-000000000002', num_episodes = 222",
		SelectQuery:    "select * from people where id = 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:         "replace no columns too few values",
		ReplaceQuery: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002')",
		ExpectedErr:  "too few values",
	},
	{
		Name:         "replace no columns too many values",
		ReplaceQuery: "replace into people values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222, 'abc')",
		ExpectedErr:  "too many values",
	},
	{
		Name:           "replace full columns",
		ReplaceQuery:   "replace into people (id, first, last, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', false, 10, 9, '00000000-0000-0000-0000-000000000002', 222)",
		SelectQuery:    "select * from people where id = 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "replace full columns mixed order",
		ReplaceQuery:   "replace into people (num_episodes, uuid, rating, age, is_married, last, first, id) values (222, '00000000-0000-0000-0000-000000000002', 9, 10, false, 'Simpson', 'Bart', 2)",
		SelectQuery:    "select * from people where id = 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name: "replace full columns negative values",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating, uuid, num_episodes) values
					    (-7, "Maggie", "Simpson", false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`,
		SelectQuery:    "select * from people where id = -7",
		ExpectedRows:   CompressRows(PeopleTestSchema, NewPeopleRowWithOptionalFields(-7, "Maggie", "Simpson", false, -1, -5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "replace full columns null values",
		ReplaceQuery:   "replace into people (id, first, last, is_married, age, rating, uuid, num_episodes) values (2, 'Bart', 'Simpson', null, null, null, null, null)",
		SelectQuery:    "select * from people where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "replace partial columns",
		ReplaceQuery:   "replace into people (id, first, last) values (2, 'Bart', 'Simpson')",
		SelectQuery:    "select id, first, last from people where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:           "replace partial columns mixed order",
		ReplaceQuery:   "replace into people (last, first, id) values ('Simpson', 'Bart', 2)",
		SelectQuery:    "select id, first, last from people where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:         "replace partial columns duplicate column",
		ReplaceQuery: "replace into people (id, first, last, first) values (2, 'Bart', 'Simpson', 'Bart')",
		ExpectedErr:  "duplicate column",
	},
	{
		Name:         "replace partial columns invalid column",
		ReplaceQuery: "replace into people (id, first, last, middle) values (2, 'Bart', 'Simpson', 'Nani')",
		ExpectedErr:  "duplicate column",
	},
	{
		Name:         "replace missing non-nullable column",
		ReplaceQuery: "replace into people (id, first) values (2, 'Bart')",
		ExpectedErr:  "column <last> received nil but is non-nullable",
	},
	{
		Name:         "replace partial columns mismatch too many values",
		ReplaceQuery: "replace into people (id, first, last) values (2, 'Bart', 'Simpson', false)",
		ExpectedErr:  "too many values",
	},
	{
		Name:         "replace partial columns mismatch too few values",
		ReplaceQuery: "replace into people (id, first, last) values (2, 'Bart')",
		ExpectedErr:  "too few values",
	},
	{
		Name:           "replace partial columns functions",
		ReplaceQuery:   "replace into people (id, first, last) values (2, UPPER('Bart'), 'Simpson')",
		SelectQuery:    "select id, first, last from people where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("BART"), types.String("Simpson"))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:         "replace partial columns multiple rows 2",
		ReplaceQuery: "replace into people (id, first, last) values (0, 'Bart', 'Simpson'), (1, 'Homer', 'Simpson')",
		SelectQuery:  "select id, first, last from people where id < 2 order by id",
		ExpectedRows: Rs(NewResultSetRow(types.Int(0), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.String("Homer"), types.String("Simpson"))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name: "replace partial columns multiple rows 5",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(9, "Jacqueline", "Bouvier", true, 80, 2),
					(10, "Patty", "Bouvier", false, 40, 7),
					(11, "Selma", "Bouvier", false, 40, 7)`,
		SelectQuery: "select id, first, last, is_married, age, rating from people where id > 6",
		ExpectedRows: CompressRows(PeopleTestSchema,
			NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
			NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			NewPeopleRow(9, "Jacqueline", "Bouvier", true, 80, 2),
			NewPeopleRow(10, "Patty", "Bouvier", false, 40, 7),
			NewPeopleRow(11, "Selma", "Bouvier", false, 40, 7),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind,
			"is_married", types.BoolKind, "age", types.IntKind, "rating", types.FloatKind),
	},
	{
		Name:         "replace partial columns multiple rows null pk",
		ReplaceQuery: "replace into people (id, first, last) values (0, 'Bart', 'Simpson'), (1, 'Homer', null)",
		ExpectedErr:  "column <last> received nil but is non-nullable",
	},
	{
		Name:           "replace partial columns multiple rows duplicate",
		ReplaceQuery:   "replace into people (id, first, last) values (2, 'Bart', 'Simpson'), (2, 'Bart', 'Simpson')",
		SelectQuery:    "select id, first, last from people where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name: "replace partial columns existing pk",
		AdditionalSetup: CreateTableFn("temppeople",
			NewSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind, "num", types.IntKind),
			NewRow(types.Int(2), types.String("Bart"), types.String("Simpson"), types.Int(44))),
		ReplaceQuery:   "replace into temppeople (id, first, last, num) values (2, 'Bart', 'Simpson', 88)",
		SelectQuery:    "select id, first, last, num from temppeople where id = 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(2), types.String("Bart"), types.String("Simpson"), types.Int(88))),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind, "num", types.IntKind),
	},
	{
		Name: "replace partial columns multiple rows replace existing pk",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 100)`,
		SelectQuery: "select id, first, last, is_married, age, rating from people where rating = 100 order by id",
		ExpectedRows: CompressRows(PeopleTestSchema,
			NewPeopleRow(0, "Homer", "Simpson", true, 45, 100),
			NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 100),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "first", types.StringKind, "last", types.StringKind,
			"is_married", types.BoolKind, "age", types.IntKind, "rating", types.FloatKind),
	},
	{
		Name: "replace partial columns multiple rows null pk",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(7, "Maggie", null, false, 1, 5.1)`,
		ExpectedErr: "Constraint failed for column 'last': Not null",
	},
	{
		Name: "type mismatch int -> string",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", 100, false, 1, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch int -> bool",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 10, 1, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch int -> uuid",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, 100)`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name: "type mismatch string -> int",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					("7", "Maggie", "Simpson", false, 1, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch string -> float",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, "5.1")`,
		ExpectedErr: "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch string -> uint",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, num_episodes) values
					(7, "Maggie", "Simpson", false, 1, "100")`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch string -> uuid",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, "a uuid but idk what im doing")`,
		ExpectedErr: "Type mismatch",
	},
	{
		Name: "type mismatch float -> string",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, 8.1, "Simpson", false, 1, 5.1)`,
		ExpectedErr: "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch float -> bool",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 0.5, 1, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch float -> int",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1.0, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch bool -> int",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(true, "Maggie", "Simpson", false, 1, 5.1)`,
		ExpectedErr:     "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch bool -> float",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, true)`,
		ExpectedErr: "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch bool -> string",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, rating) values
					(7, true, "Simpson", false, 1, 5.1)`,
		ExpectedErr: "Type mismatch",
		SkipOnSqlEngine: true,
	},
	{
		Name: "type mismatch bool -> uuid",
		ReplaceQuery: `replace into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, true)`,
		ExpectedErr: "Type mismatch",
	},
}
