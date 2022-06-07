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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleSelectQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenSelect = true

// Structure for a test of a select query
type SelectTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The query to run, excluding an ending semicolon
	Query string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The schema of the result of the query, nil if an error is expected. Mutually exclusive with ExpectedSchema. Use if
	// the schema is difficult to specify with dolt schemas.
	ExpectedSqlSchema sql.Schema
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

// We are doing structural equality tests on time.Time values in some of these
// tests. The SQL value layer works with times in time.Local location, but
// go standard library will return different values (which will have the same
// behavior) depending on whether detailed timezone information has been loaded
// for time.Local already. Here, we always load the detailed information so
// that our structural equality tests will be reliable.
var loadedLocalLocation *time.Location

func LoadedLocalLocation() *time.Location {
	var err error
	loadedLocalLocation, err = time.LoadLocation(time.Local.String())
	if err != nil {
		panic(err)
	}
	if loadedLocalLocation == nil {
		panic("nil LoadedLocalLocation " + time.Local.String())
	}
	return loadedLocalLocation
}

// BasicSelectTests cover basic select statement features and error handling
func BasicSelectTests() []SelectTest {
	headCommitHash := "73hc2robs4v0kt9taoe3m5hd49dmrgun"
	if types.Format_Default == types.Format_DOLT_DEV {
		headCommitHash = "hqcfvhaumjtetijsqdnnjhj87f0lesbo"
	}
	return []SelectTest{
		{
			Name:           "select * on primary key",
			Query:          "select * from people where id = 2",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select * ",
			Query:          "select * from people",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 1",
			Query:          "select * from people limit 1",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 1 offset 0",
			Query:          "select * from people limit 0,1",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 1 offset 1",
			Query:          "select * from people limit 1 offset 1;",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 1 offset 5",
			Query:          "select * from people limit 5,1",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 1 offset 6",
			Query:          "select * from people limit 6,1",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 0",
			Query:          "select * from people limit 0",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, limit 0 offset 0",
			Query:          "select * from people limit 0,0",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:        "select *, limit -1",
			Query:       "select * from people limit -1",
			ExpectedErr: "Limit must be >= 0 if supplied",
		},
		{
			Name:        "select *, offset -1",
			Query:       "select * from people limit -1,1",
			ExpectedErr: "Offset must be >= 0 if supplied",
		},
		{
			Name:           "select *, limit 100",
			Query:          "select * from people limit 100",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < int",
			Query:          "select * from people where age < 40",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < int, limit 1",
			Query:          "select * from people where age < 40 limit 1",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < int, limit 2",
			Query:          "select * from people where age < 40 limit 2",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < int, limit 100",
			Query:          "select * from people where age < 40 limit 100",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by int",
			Query:          "select * from people order by id",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by int desc",
			Query:          "select * from people order by id desc",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney, Moe, Lisa, Bart, Marge, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by float",
			Query:          "select * from people order by rating",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney, Moe, Marge, Homer, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by string",
			Query:          "select * from people order by first_name",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney, Bart, Homer, Lisa, Marge, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by string,string",
			Query:          "select * from people order by last_name desc, first_name asc",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Bart, Homer, Lisa, Marge, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by with limit",
			Query:          "select * from people order by first_name limit 2",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney, Bart),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, order by string,string with limit",
			Query:          "select * from people order by last_name desc, first_name asc limit 2",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Bart),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where > int reversed",
			Query:          "select * from people where 40 > age",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where <= int",
			Query:          "select * from people where age <= 40",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where >= int reversed",
			Query:          "select * from people where 40 >= age",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where > int",
			Query:          "select * from people where age > 40",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < int reversed",
			Query:          "select * from people where 40 < age",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where >= int",
			Query:          "select * from people where age >= 40",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where <= int reversed",
			Query:          "select * from people where 40 <= age",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where > string",
			Query:          "select * from people where last_name > 'Simpson'",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < string",
			Query:          "select * from people where last_name < 'Simpson'",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where = string",
			Query:          "select * from people where last_name = 'Simpson'",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where > float",
			Query:          "select * from people where rating > 8.0 order by id",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < float",
			Query:          "select * from people where rating < 8.0",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where = float",
			Query:          "select * from people where rating = 8.0",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where < float reversed",
			Query:          "select * from people where 8.0 < rating",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where > float reversed",
			Query:          "select * from people where 8.0 > rating",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where = float reversed",
			Query:          "select * from people where 8.0 = rating",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where bool = ",
			Query:          "select * from people where is_married = true",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where bool = false ",
			Query:          "select * from people where is_married = false",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where bool <> ",
			Query:          "select * from people where is_married <> false",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, where bool",
			Query:          "select * from people where is_married",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, and clause",
			Query:          "select * from people where is_married and age > 38",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, or clause",
			Query:          "select * from people where is_married or age < 20",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause string",
			Query:          "select * from people where first_name in ('Homer', 'Marge')",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause integer",
			Query:          "select * from people where age in (-10, 40)",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause float",
			Query:          "select * from people where rating in (-10.0, 8.5)",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause, mixed types",
			Query:          "select * from people where first_name in ('Homer', 40)",
			ExpectedSchema: CompressSchema(PeopleTestSchema),
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
		},
		{
			Name:           "select *, in clause, mixed numeric types",
			Query:          "select * from people where age in (-10.0, 40)",
			ExpectedSchema: CompressSchema(PeopleTestSchema),
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
		},
		{
			Name:           "select *, not in clause",
			Query:          "select * from people where first_name not in ('Homer', 'Marge')",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause single element",
			Query:          "select * from people where first_name in ('Homer')",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, in clause single type mismatch",
			Query:          "select * from people where first_name in (1.0)",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is null clause ",
			Query:          "select * from people where uuid is null",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is not null clause ",
			Query:          "select * from people where uuid is not null",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is true clause ",
			Query:          "select * from people where is_married is true",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is not true clause ",
			Query:          "select * from people where is_married is not true",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is false clause ",
			Query:          "select * from people where is_married is false",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is not false clause ",
			Query:          "select * from people where is_married is not false",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Marge),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, is true clause on non-bool column",
			Query:          "select * from people where age is true",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, AllPeopleRows...),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "binary expression in select",
			Query:          "select age + 1 as a from people where is_married order by a",
			ExpectedRows:   ToSqlRows(NewResultSetSchema("a", types.IntKind), NewResultSetRow(types.Int(39)), NewResultSetRow(types.Int(41))),
			ExpectedSchema: NewResultSetSchema("a", types.IntKind),
		},
		{
			Name:         "and expression in select",
			Query:        "select is_married and age >= 40 from people where last_name = 'Simpson' order by id limit 2",
			ExpectedRows: []sql.Row{{true}, {false}},
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{Name: "is_married and age >= 40", Type: sql.Int8},
			},
		},
		{
			Name:  "or expression in select",
			Query: "select first_name, age <= 10 or age >= 40 as not_marge from people where last_name = 'Simpson' order by id desc",
			ExpectedRows: []sql.Row{
				{"Lisa", true},
				{"Bart", true},
				{"Marge", false},
				{"Homer", true},
			},
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{Name: "first_name", Type: typeinfo.StringDefaultType.ToSqlType()},
				&sql.Column{Name: "not_marge", Type: sql.Int8},
			},
		},
		{
			Name:           "unary expression in select",
			Query:          "select -age as age from people where is_married order by age",
			ExpectedRows:   ToSqlRows(NewResultSetSchema("age", types.IntKind), NewResultSetRow(types.Int(-40)), NewResultSetRow(types.Int(-38))),
			ExpectedSchema: NewResultSetSchema("age", types.IntKind),
		},
		{
			Name:            "unary expression in select, alias named after column",
			Query:           "select -age as age from people where is_married order by people.age",
			ExpectedRows:    ToSqlRows(NewResultSetSchema("age", types.IntKind), NewResultSetRow(types.Int(-38)), NewResultSetRow(types.Int(-40))),
			ExpectedSchema:  NewResultSetSchema("age", types.IntKind),
			SkipOnSqlEngine: true, // this seems to be a bug in the engine
		},
		{
			Name:           "select *, -column",
			Query:          "select * from people where -rating = -8.5",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, -column, string type",
			Query:          "select * from people where -first_name = 'Homer'",
			ExpectedSchema: CompressSchema(PeopleTestSchema),
			ExpectedRows:   ToSqlRows(PeopleTestSchema, AllPeopleRows...), // A little weird, but correct due to mysql type conversion rules (both expression evaluate to 0 after conversion)
		},
		{
			Name:           "select *, binary + in where",
			Query:          "select * from people where age + 1 = 41",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, binary - in where",
			Query:          "select * from people where age - 1 = 39",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, binary / in where",
			Query:          "select * from people where age / 2 = 20",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, binary * in where",
			Query:          "select * from people where age * 2 = 80",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, binary % in where",
			Query:          "select * from people where age % 4 = 0",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Lisa, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select *, complex binary expr in where",
			Query:          "select * from people where age / 4 + 2 * 2 = 14",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:        "select *, binary + in where type mismatch",
			Query:       "select * from people where first_name + 1 = 41",
			ExpectedErr: "Type mismatch evaluating expression 'first_name + 1'",
		},
		{
			Name:        "select *, binary - in where type mismatch",
			Query:       "select * from people where first_name - 1 = 39",
			ExpectedErr: "Type mismatch evaluating expression 'first_name - 1'",
		},
		{
			Name:        "select *, binary / in where type mismatch",
			Query:       "select * from people where first_name / 2 = 20",
			ExpectedErr: "Type mismatch evaluating expression 'first_name / 2'",
		},
		{
			Name:        "select *, binary * in where type mismatch",
			Query:       "select * from people where first_name * 2 = 80",
			ExpectedErr: "Type mismatch evaluating expression 'first_name * 2'",
		},
		{
			Name:        "select *, binary % in where type mismatch",
			Query:       "select * from people where first_name % 4 = 0",
			ExpectedErr: "Type mismatch evaluating expression 'first_name % 4'",
		},
		{
			Name:           "select * with where, order by",
			Query:          "select * from people where `uuid` is not null and first_name <> 'Marge' order by last_name desc, age",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Moe, Lisa, Bart, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "select subset of cols",
			Query:          "select first_name, last_name from people where age >= 40",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name", "last_name"), Homer, Moe, Barney),
			ExpectedSchema: CompressSchema(PeopleTestSchema, "first_name", "last_name"),
		},
		{
			Name:           "column aliases",
			Query:          "select first_name as f, last_name as l from people where age >= 40",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name", "last_name"), Homer, Moe, Barney),
			ExpectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			Name:           "duplicate column aliases",
			Query:          "select first_name as f, last_name as f from people where age >= 40",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name", "last_name"), Homer, Moe, Barney),
			ExpectedSchema: NewResultSetSchema("f", types.StringKind, "f", types.StringKind),
		},
		{
			Name:  "column selected more than once",
			Query: "select first_name, first_name from people where age >= 40 order by id",
			ExpectedRows: []sql.Row{
				{"Homer", "Homer"},
				{"Moe", "Moe"},
				{"Barney", "Barney"},
			},
			ExpectedSchema: NewResultSetSchema("first_name", types.StringKind, "first_name", types.StringKind),
		},
		{
			Name:        "duplicate table selection",
			Query:       "select first_name as f, last_name as f from people, people where age >= 40",
			ExpectedErr: "Non-unique table name / alias: people",
		},
		{
			Name:        "duplicate table alias",
			Query:       "select * from people p, people p where age >= 40",
			ExpectedErr: "Non-unique table name / alias: 'p'",
		},
		{
			Name:            "column aliases in where clause",
			Query:           `select first_name as f, last_name as l from people where f = "Homer"`,
			ExpectedErr:     "Unknown column: 'f'",
			SkipOnSqlEngine: true, // this is actually a bug (aliases aren't usable in filters)
		},
		{
			Name:           "select subset of columns with order by",
			Query:          "select first_name from people order by age, first_name",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name"), Lisa, Bart, Marge, Barney, Homer, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema, "first_name"),
		},
		{
			Name:           "column aliases with order by",
			Query:          "select first_name as f from people order by age, f",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name"), Lisa, Bart, Marge, Barney, Homer, Moe),
			ExpectedSchema: NewResultSetSchema("f", types.StringKind),
		},
		{
			Name:            "ambiguous column in order by",
			Query:           "select first_name as f, last_name as f from people order by f",
			ExpectedErr:     "Ambiguous column: 'f'",
			SkipOnSqlEngine: true, // this is a bug in go-mysql-server
		},
		{
			Name:           "table aliases",
			Query:          "select p.first_name as f, p.last_name as l from people p where p.first_name = 'Homer'",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name", "last_name"), Homer),
			ExpectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			Name:           "table aliases without column aliases",
			Query:          "select p.first_name, p.last_name from people p where p.first_name = 'Homer'",
			ExpectedRows:   ToSqlRows(SubsetSchema(PeopleTestSchema, "first_name", "last_name"), Homer),
			ExpectedSchema: NewResultSetSchema("first_name", types.StringKind, "last_name", types.StringKind),
		},
		{
			Name:        "table aliases with bad alias",
			Query:       "select m.first_name as f, p.last_name as l from people p where p.f = 'Homer'",
			ExpectedErr: "Unknown table: 'm'",
		},
		{
			Name: "column aliases, all columns",
			Query: `select id as i, first_name as f, last_name as l, is_married as m, age as a,
						rating as r, uuid as u, num_episodes as n from people
						where age >= 40`,
			ExpectedRows: ToSqlRows(PeopleTestSchema, Homer, Moe, Barney),
			ExpectedSchema: NewResultSetSchema("i", types.IntKind, "f", types.StringKind,
				"l", types.StringKind, "m", types.IntKind, "a", types.IntKind, "r", types.FloatKind,
				"u", types.StringKind, "n", types.UintKind),
		},
		{
			Name:           "select *, not equals",
			Query:          "select * from people where age <> 40",
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Marge, Bart, Lisa, Moe),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "empty result set",
			Query:          "select * from people where age > 80",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			Name:           "empty result set with columns",
			Query:          "select id, age from people where age > 80",
			ExpectedRows:   ToSqlRows(PeopleTestSchema),
			ExpectedSchema: CompressSchema(PeopleTestSchema, "id", "age"),
		},
		{
			Name:        "unknown table",
			Query:       "select * from dne",
			ExpectedErr: `Unknown table: 'dne'`,
		},
		{
			Name:        "unknown diff table",
			Query:       "select * from dolt_diff_dne",
			ExpectedErr: `Unknown table: 'dolt_diff_dne'`,
		},
		{
			Name:        "unknown diff table",
			Query:       "select * from dolt_commit_diff_dne",
			ExpectedErr: `Unknown table: 'dolt_commit_diff_dne'`,
		},
		{
			Name:        "unknown history table",
			Query:       "select * from dolt_history_dne",
			ExpectedErr: `Unknown table: 'dolt_history_dne'`,
		},
		{
			Name:        "unknown table in join",
			Query:       "select * from people join dne",
			ExpectedErr: `Unknown table: 'dne'`,
		},
		{
			Name:  "no table",
			Query: "select 1",
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{
					Name: "1",
					Type: sql.Int8,
				},
			},
			ExpectedRows: []sql.Row{{int8(1)}},
		},
		{
			Name:        "unknown column in where",
			Query:       "select * from people where dne > 8.0",
			ExpectedErr: `Unknown column: 'dne'`,
		},
		{
			Name:        "unknown column in order by",
			Query:       "select * from people where rating > 8.0 order by dne",
			ExpectedErr: `Unknown column: 'dne'`,
		},
		{
			Name:        "unsupported comparison",
			Query:       "select * from people where function(first_name)",
			ExpectedErr: "not supported",
		},
		{
			Name:           "type mismatch in where clause",
			Query:          `select * from people where id = "0"`,
			ExpectedSchema: CompressSchema(PeopleTestSchema),
			ExpectedRows:   ToSqlRows(PeopleTestSchema, Homer),
		},
		{
			Name:  "select * from log system table",
			Query: "select * from dolt_log",
			ExpectedRows: []sql.Row{
				{
					headCommitHash,
					"billy bob",
					"bigbillieb@fake.horse",
					time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).In(LoadedLocalLocation()),
					"Initialize data repository",
				},
			},
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{Name: "commit_hash", Type: sql.Text},
				&sql.Column{Name: "committer", Type: sql.Text},
				&sql.Column{Name: "email", Type: sql.Text},
				&sql.Column{Name: "date", Type: sql.Datetime},
				&sql.Column{Name: "message", Type: sql.Text},
			},
		},
		{
			Name:         "select * from conflicts system table",
			Query:        "select * from dolt_conflicts",
			ExpectedRows: []sql.Row{},
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{Name: "table", Type: sql.Text},
				&sql.Column{Name: "num_conflicts", Type: sql.Uint64},
			},
		},
		{
			Name:  "select * from branches system table",
			Query: "select * from dolt_branches",
			ExpectedRows: []sql.Row{
				{
					env.DefaultInitBranch,
					headCommitHash,
					"billy bob", "bigbillieb@fake.horse",
					time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).In(LoadedLocalLocation()),
					"Initialize data repository",
				},
			},
			ExpectedSqlSchema: sql.Schema{
				&sql.Column{Name: "name", Type: sql.Text},
				&sql.Column{Name: "hash", Type: sql.Text},
				&sql.Column{Name: "latest_committer", Type: sql.Text},
				&sql.Column{Name: "latest_committer_email", Type: sql.Text},
				&sql.Column{Name: "latest_commit_date", Type: sql.Datetime},
				&sql.Column{Name: "latest_commit_message", Type: sql.Text},
			},
		},
	}
}

var sqlDiffSchema = sql.Schema{
	&sql.Column{Name: "to_id", Type: sql.Int64},
	&sql.Column{Name: "to_first_name", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "to_last_name", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "to_addr", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "from_id", Type: sql.Int64},
	&sql.Column{Name: "from_first_name", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "from_last_name", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "from_addr", Type: typeinfo.StringDefaultType.ToSqlType()},
	&sql.Column{Name: "diff_type", Type: typeinfo.StringDefaultType.ToSqlType()},
}

var SelectDiffTests = []SelectTest{
	{
		Name:  "select from diff system table",
		Query: "select to_id, to_first_name, to_last_name, to_addr, from_id, from_first_name, from_last_name, from_addr, diff_type from dolt_diff_test_table",
		ExpectedRows: ToSqlRows(DiffSchema,
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(6), 1: types.String("Katie"), 2: types.String("McCulloch"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 7: types.Int(0), 8: types.String("Aaron"), 9: types.String("Son"), 10: types.String("123 Fake St"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 7: types.Int(1), 8: types.String("Brian"), 9: types.String("Hendriks"), 10: types.String("456 Bull Ln"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 7: types.Int(2), 8: types.String("Tim"), 9: types.String("Sehn"), 10: types.String("789 Not Real Ct"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 7: types.Int(3), 8: types.String("Zach"), 9: types.String("Musgrave"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele"), 14: types.String("added")})),
		),
		ExpectedSqlSchema: sqlDiffSchema,
	},
	{
		Name:  "select from diff system table with to commit",
		Query: "select to_id, to_first_name, to_last_name, to_addr, from_id, from_first_name, from_last_name, from_addr, diff_type from dolt_diff_test_table where to_commit = 'WORKING'",
		ExpectedRows: ToSqlRows(DiffSchema,
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(6), 1: types.String("Katie"), 2: types.String("McCulloch"), 14: types.String("added")})),
		),
		ExpectedSqlSchema: sqlDiffSchema,
	},
	// TODO: fix dependencies to hashof function can be registered and used here, also create branches when generating the history so that different from and to commits can be tested.
	/*{
		Name:  "select from diff system table with from and to commit and test insensitive name",
		Query: "select to_id, to_first_name, to_last_name, to_addr, to_age_4, to_age_5, from_id, from_first_name, from_last_name, from_addr, from_age_4, from_age_5, diff_type from dolt_diff_TeSt_TaBlE where from_commit = 'add-age' and to_commit = 'main'",
		ExpectedRows: ToSqlRows(DiffSchema,
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{7: types.Int(0), 8: types.String("Aaron"), 9: types.String("Son"), 11: types.Int(35), 0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 5: types.Uint(35), 13: types.String("add-age"), 6: types.String("main"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{7: types.Int(1), 8: types.String("Brian"), 9: types.String("Hendriks"), 11: types.Int(38), 0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 5: types.Uint(38), 13: types.String("add-age"), 6: types.String("main"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{7: types.Int(2), 8: types.String("Tim"), 9: types.String("Sehn"), 11: types.Int(37), 0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 5: types.Uint(37), 13: types.String("add-age"), 6: types.String("main"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{7: types.Int(3), 8: types.String("Zach"), 9: types.String("Musgrave"), 11: types.Int(37), 0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 5: types.Uint(37), 13: types.String("add-age"), 6: types.String("main"), 14: types.String("modified")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele"), 3: types.NullValue, 13: types.String("add-age"), 6: types.String("main"), 14: types.String("added")})),
			mustRow(row.New(types.Format_Default, DiffSchema, row.TaggedValues{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins"), 3: types.NullValue, 13: types.String("add-age"), 6: types.String("main"), 14: types.String("added")})),
		),
		ExpectedSqlSchema: sqlDiffSchema,
	},*/
}

var AsOfTests = []SelectTest{
	{
		Name:  "select * from seed branch",
		Query: "select * from test_table as of 'seed'",
		ExpectedRows: ToSqlRows(InitialHistSch,
			mustRow(row.New(types.Format_Default, InitialHistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son")})),
			mustRow(row.New(types.Format_Default, InitialHistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks")})),
			mustRow(row.New(types.Format_Default, InitialHistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn")})),
		),
		ExpectedSchema: InitialHistSch,
	},
	{
		Name:  "select * from add-age branch",
		Query: "select * from test_table as of 'add-age'",
		ExpectedRows: ToSqlRows(AddAgeAt4HistSch,
			mustRow(row.New(types.Format_Default, AddAgeAt4HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 4: types.Int(35)})),
			mustRow(row.New(types.Format_Default, AddAgeAt4HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 4: types.Int(38)})),
			mustRow(row.New(types.Format_Default, AddAgeAt4HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 4: types.Int(37)})),
			mustRow(row.New(types.Format_Default, AddAgeAt4HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 4: types.Int(37)})),
		),
		ExpectedSchema: AddAgeAt4HistSch,
	},
	{
		Name:  "select * from main branch",
		Query: "select * from test_table as of 'main'",
		ExpectedRows: ToSqlRows(ReaddAgeAt5HistSch,
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 5: types.Uint(35)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 5: types.Uint(38)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins")})),
		),
		ExpectedSchema: ReaddAgeAt5HistSch,
	},
	{
		Name:  "select * from HEAD~",
		Query: "select * from test_table as of 'HEAD~'",
		ExpectedRows: ToSqlRows(AddAddrAt3HistSch,
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
		),
		ExpectedSchema: AddAddrAt3HistSch,
	},
	{
		Name:  "select * from HEAD^",
		Query: "select * from test_table as of 'HEAD^'",
		ExpectedRows: ToSqlRows(AddAddrAt3HistSch,
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
		),
		ExpectedSchema: AddAddrAt3HistSch,
	},
	{
		Name:  "select * from main^",
		Query: "select * from test_table as of 'main^'",
		ExpectedRows: ToSqlRows(AddAddrAt3HistSch,
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
		),
		ExpectedSchema: AddAddrAt3HistSch,
	},
	// Because of an implementation detail in the way we process history for test setup, each commit is 2 hours apart.
	{
		Name:  "select * from timestamp after HEAD",
		Query: "select * from test_table as of CONVERT('1970-01-01 10:00:00', DATETIME)",
		ExpectedRows: ToSqlRows(ReaddAgeAt5HistSch,
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 5: types.Uint(35)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 5: types.Uint(38)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins")})),
		),
		ExpectedSchema: ReaddAgeAt5HistSch,
	},
	{
		Name:  "select * from timestamp, HEAD exact",
		Query: "select * from test_table as of CONVERT('1970-01-01 08:00:00', DATETIME)",
		ExpectedRows: ToSqlRows(ReaddAgeAt5HistSch,
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St"), 5: types.Uint(35)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln"), 5: types.Uint(38)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave"), 3: types.String("-1 Imaginary Wy"), 5: types.Uint(37)})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
			mustRow(row.New(types.Format_Default, ReaddAgeAt5HistSch, row.TaggedValues{0: types.Int(5), 1: types.String("Daylon"), 2: types.String("Wilkins")})),
		),
		ExpectedSchema: ReaddAgeAt5HistSch,
	},
	{
		Name:  "select * from timestamp, HEAD~ + 1",
		Query: "select * from test_table as of CONVERT('1970-01-01 07:00:00', DATETIME)",
		ExpectedRows: ToSqlRows(AddAddrAt3HistSch,
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
		),
		ExpectedSchema: AddAddrAt3HistSch,
	},
	{
		Name:  "select * from timestamp, HEAD~",
		Query: "select * from test_table as of CONVERT('1970-01-01 06:00:00', DATETIME)",
		ExpectedRows: ToSqlRows(AddAddrAt3HistSch,
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(0), 1: types.String("Aaron"), 2: types.String("Son"), 3: types.String("123 Fake St")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(1), 1: types.String("Brian"), 2: types.String("Hendriks"), 3: types.String("456 Bull Ln")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(2), 1: types.String("Tim"), 2: types.String("Sehn"), 3: types.String("789 Not Real Ct")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(3), 1: types.String("Zach"), 2: types.String("Musgrave")})),
			mustRow(row.New(types.Format_Default, AddAddrAt3HistSch, row.TaggedValues{0: types.Int(4), 1: types.String("Matt"), 2: types.String("Jesuele")})),
		),
		ExpectedSchema: AddAddrAt3HistSch,
	},
	{
		Name:        "select * from timestamp, before table creation",
		Query:       "select * from test_table as of CONVERT('1970-01-01 02:00:00', DATETIME)",
		ExpectedErr: "not found",
	},
}

// SQL is supposed to be case insensitive. These are tests of that promise.
// Many of these are currently broken in go-myqsl-server. The queries return the correct results in all cases, but the
// column names in the result schemas often have the wrong case. They sometimes use the case from the table, rather
// than the case of the expression in the query (the correct behavior). This is a minor issue, but we should fix it
// eventually.
var CaseSensitivityTests = []SelectTest{
	{
		Name: "table name has mixed case, select lower case",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:          "select test from mixedcase",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   []sql.Row{{"1"}},
	},
	{
		Name: "table name has mixed case, select upper case",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:          "select test from MIXEDCASE",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   []sql.Row{{"1"}},
	},
	{
		Name: "qualified select *",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:          "select mixedcAse.* from MIXEDCASE",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   []sql.Row{{"1"}},
	},
	{
		Name: "qualified select column",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:           "select mixedcAse.TeSt from MIXEDCASE",
		ExpectedSchema:  NewResultSetSchema("TeSt", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "table alias select *",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:          "select Mc.* from MIXEDCASE as mc",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   []sql.Row{{"1"}},
	},
	{
		Name: "table alias select column",
		AdditionalSetup: CreateTableWithRowsFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:           "select mC.TeSt from MIXEDCASE as MC",
		ExpectedSchema:  NewResultSetSchema("TeSt", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "multiple tables with the same case-insensitive name, exact match",
		AdditionalSetup: Compose(
			// the table name passed to NewSchemaForTable isn't important, except to get unique tags
			CreateTableWithRowsFn("tableName", NewSchemaForTable("tableName1", "test", types.StringKind), []types.Value{types.String("1")}),
			CreateTableWithRowsFn("TABLENAME", NewSchemaForTable("TABLENAME2", "test", types.StringKind)),
			CreateTableWithRowsFn("tablename", NewSchemaForTable("tablename3", "test", types.StringKind)),
		),
		Query:          "select test from tableName",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   []sql.Row{{"1"}},
	},
	{
		Name: "alias with same name as table",
		AdditionalSetup: Compose(
			CreateTableWithRowsFn("tableName", NewSchema("test", types.StringKind)),
			CreateTableWithRowsFn("other", NewSchema("othercol", types.StringKind)),
		),
		Query:       "select other.test from tablename as other, other",
		ExpectedErr: "Non-unique table name / alias: 'other'",
	},
	{
		Name: "two table aliases with same name",
		AdditionalSetup: Compose(
			CreateTableWithRowsFn("tableName", NewSchema("test", types.StringKind)),
			CreateTableWithRowsFn("other", NewSchema("othercol", types.StringKind)),
		),
		Query:       "select bad.test from tablename as bad, other as bad",
		ExpectedErr: "Non-unique table name / alias: 'bad'",
	},
	{
		Name: "column name has mixed case, select lower case",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema("MiXeDcAsE", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:           "select mixedcase from test",
		ExpectedSchema:  NewResultSetSchema("mixedcase", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "column name has mixed case, select upper case",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema("MiXeDcAsE", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:           "select MIXEDCASE from test",
		ExpectedSchema:  NewResultSetSchema("MIXEDCASE", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "select with multiple matching columns, exact match",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			[]types.Value{types.String("1"), types.String("2")}),
		Query:           "select mixedcase from test",
		ExpectedSchema:  NewResultSetSchema("mixedcase", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true, // TODO: table should be illegal. field names cannot be the same case-insensitive
	},
	{
		Name: "column is reserved word, select not backticked",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			[]types.Value{types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")}),
		Query:          "select Timestamp from test",
		ExpectedRows:   []sql.Row{{"1"}},
		ExpectedSchema: NewResultSetSchema("Timestamp", types.StringKind),
	},
	{
		Name: "column is reserved word, qualified with table alias",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			[]types.Value{types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")}),
		Query:          "select t.Timestamp from test as t",
		ExpectedRows:   []sql.Row{{"1"}},
		ExpectedSchema: NewResultSetSchema("Timestamp", types.StringKind),
	},
	{
		Name: "column is reserved word, select not backticked #2",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema("YeAr", types.StringKind),
			[]types.Value{types.String("1")}),
		Query:           "select Year from test",
		ExpectedSchema:  NewResultSetSchema("Year", types.StringKind),
		ExpectedRows:    []sql.Row{{"1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "column is reserved word, select backticked",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			[]types.Value{types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")}),
		Query:           "select `Timestamp` from test",
		ExpectedRows:    []sql.Row{{"1"}},
		ExpectedSchema:  NewResultSetSchema("Timestamp", types.StringKind),
		SkipOnSqlEngine: true,
	},
	{
		Name: "column is reserved word, select backticked #2",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema(
				"Year", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			[]types.Value{types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")}),
		Query: "select `Year`, `OR`, `SELect`, `anD` from test",
		ExpectedSchema: NewResultSetSchema(
			"Year", types.StringKind,
			"OR", types.StringKind,
			"SELect", types.StringKind,
			"anD", types.StringKind),
		ExpectedRows:    []sql.Row{{"1", "aaa", "create", "1.1"}},
		SkipOnSqlEngine: true,
	},
	{
		Name: "column is reserved word, table qualified",
		AdditionalSetup: CreateTableWithRowsFn("test",
			NewSchema(
				"Year", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			[]types.Value{types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")}),
		Query: "select Year, t.OR, t.SELect, t.anD from test t",
		ExpectedSchema: NewResultSetSchema(
			"Year", types.StringKind,
			"OR", types.StringKind,
			"SELect", types.StringKind,
			"anD", types.StringKind),
		ExpectedRows:    []sql.Row{{"1", "aaa", "create", "1.1"}},
		SkipOnSqlEngine: true,
	},
}

// Tests of join functionality, basically any query involving more than one table should go here for now.
var JoinTests = []SelectTest{
	{
		Name:  "Full cross product",
		Query: `select * from people, episodes`,
		ExpectedRows: ToSqlRows(CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep4),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep4),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep4),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep4),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep4),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep4),
		),
		ExpectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
	},
	{
		Name:  "Natural join with where clause",
		Query: `select * from people p, episodes e where e.id = p.id`,
		ExpectedRows: ToSqlRows(CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep4),
		),
		ExpectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
	},
	{
		Name:  "Three table natural join with where clause",
		Query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
		ExpectedRows: ToSqlRows(CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep3),
		),
		ExpectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
	},
	// TODO: error messages are different in SQL engine
	{
		Name:        "ambiguous column in select",
		Query:       `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
		ExpectedErr: "Ambiguous column: 'id'",
	},
	{
		Name:        "ambiguous column in where",
		Query:       `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
		ExpectedErr: "Ambiguous column: 'id'",
	},
	{
		Name:  "Natural join with where clause, select subset of columns",
		Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p, episodes e where e.id = p.id`,
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with where clause and column aliases",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p, episodes e where e.id = p.id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with where clause and quoted column alias",
		Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p, episodes e where e.id = p.id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
			"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with join clause",
		Query: `select * from people p join episodes e on e.id = p.id`,
		ExpectedRows: ToSqlRows(CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep4),
		),
		ExpectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
	},
	{
		Name:  "Three table natural join with join clause",
		Query: `select p.*, e.* from people p join appearances a on a.character_id = p.id join episodes e on a.episode_id = e.id`,
		ExpectedRows: ToSqlRows(CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Homer, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
			ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
			ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep2),
			ConcatRows(PeopleTestSchema, Barney, EpisodesTestSchema, Ep3),
		),
		ExpectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
	},
	{
		Name:  "Natural join with join clause, select subset of columns",
		Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p join episodes e on e.id = p.id`,
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with join clause, select subset of columns, join columns not selected",
		Query: `select e.name, p.first_name, p.last_name from people p join episodes e on e.id = p.id`,
		ExpectedRows: ToSqlRows(NewResultSetSchema("name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "Natural join with join clause, select subset of columns, order by clause",
		Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p 
							join episodes e on e.id = p.id
							order by e.name`,
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "Natural join with join clause, select subset of columns, order by clause on non-selected column",
		Query: `select e.id, p.id, e.name, p.first_name, p.last_name from people p 
							join episodes e on e.id = p.id
							order by age`,
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and column aliases",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and column aliases, order by",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id order by ename",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and quoted column alias",
		Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first_name as pfirst_name, p.last_name last_name from people p join episodes e on e.id = p.id",
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
			"ename", types.StringKind, "pfirst_name", types.StringKind, "last_name", types.StringKind),
	},
	{
		Name: "Join from table with two key columns to table with one key column",
		Query: `select a.episode_id as eid, p.id as pid, p.first_name
						from appearances a join people p on a.character_id = p.id order by eid, pid`,
		ExpectedRows: ToSqlRows(
			NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"first_name", types.StringKind),
			NewResultSetRow(types.Int(1), types.Int(0), types.String("Homer")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Marge")),
			NewResultSetRow(types.Int(2), types.Int(0), types.String("Homer")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart")),
			NewResultSetRow(types.Int(2), types.Int(3), types.String("Lisa")),
			NewResultSetRow(types.Int(2), types.Int(4), types.String("Moe")),
			NewResultSetRow(types.Int(3), types.Int(0), types.String("Homer")),
			NewResultSetRow(types.Int(3), types.Int(1), types.String("Marge")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Lisa")),
			NewResultSetRow(types.Int(3), types.Int(5), types.String("Barney")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"first_name", types.StringKind),
	},
}

func TestSelect(t *testing.T) {
	for _, test := range BasicSelectTests() {
		t.Run(test.Name, func(t *testing.T) {
			testSelectQuery(t, test)
		})
	}
}

func TestDiffQueries(t *testing.T) {
	for _, test := range SelectDiffTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectDiffQuery(t, test)
		})
	}
}

func TestAsOfQueries(t *testing.T) {
	for _, test := range AsOfTests {
		t.Run(test.Name, func(t *testing.T) {
			// AS OF queries use the same history as the diff tests, so exercise the same test setup
			testSelectDiffQuery(t, test)
		})
	}
}

func TestJoins(t *testing.T) {
	for _, tt := range JoinTests {
		if tt.Name == "Join from table with two key columns to table with one key column" {
			t.Run(tt.Name, func(t *testing.T) {
				testSelectQuery(t, tt)
			})
		}
	}
}

// Tests of case sensitivity handling
func TestCaseSensitivity(t *testing.T) {
	for _, tt := range CaseSensitivityTests {
		t.Run(tt.Name, func(t *testing.T) {
			testSelectQuery(t, tt)
		})
	}
}

var systemTableSelectTests = []SelectTest{
	{
		Name: "select from dolt_docs",
		AdditionalSetup: CreateTableFn("dolt_docs",
			doltdocs.DocsSchema,
			NewRowWithSchema(doltdocs.DocsSchema,
				types.String("LICENSE.md"),
				types.String("A license")),
		),
		Query: "select * from dolt_docs",
		ExpectedRows: ToSqlRows(CompressSchema(doltdocs.DocsSchema),
			NewRow(types.String("LICENSE.md"), types.String("A license"))),
		ExpectedSchema: CompressSchema(doltdocs.DocsSchema),
	},
	{
		Name: "select from dolt_query_catalog",
		AdditionalSetup: CreateTableFn(doltdb.DoltQueryCatalogTableName,
			dtables.DoltQueryCatalogSchema,
			NewRowWithSchema(dtables.DoltQueryCatalogSchema,
				types.String("existingEntry"),
				types.Uint(2),
				types.String("example"),
				types.String("select 2+2 from dual"),
				types.String("description")),
		),
		Query: "select * from dolt_query_catalog",
		ExpectedRows: ToSqlRows(CompressSchema(dtables.DoltQueryCatalogSchema),
			NewRow(types.String("existingEntry"), types.Uint(2), types.String("example"), types.String("select 2+2 from dual"), types.String("description")),
		),
		ExpectedSchema: CompressSchema(dtables.DoltQueryCatalogSchema),
	},
	{
		Name: "select from dolt_schemas",
		AdditionalSetup: CreateTableFn(doltdb.SchemasTableName,
			SchemasTableSchema(),
			NewRowWithSchema(SchemasTableSchema(),
				types.String("view"),
				types.String("name"),
				types.String("select 2+2 from dual"),
				types.Int(1),
				CreateTestJSON(),
			)),
		Query: "select * from dolt_schemas",
		ExpectedRows: ToSqlRows(CompressSchema(SchemasTableSchema()),
			NewRow(types.String("view"), types.String("name"), types.String("select 2+2 from dual"), types.Int(1), CreateTestJSON()),
		),
		ExpectedSchema: CompressSchema(SchemasTableSchema()),
	},
}

func CreateTestJSON() types.JSON {
	vrw := types.NewMemoryValueStore()
	extraJSON, _ := types.NewMap(nil, vrw, types.String("CreatedAt"), types.Float(1))
	res, _ := types.NewJSONDoc(types.Format_Default, vrw, extraJSON)
	return res
}

func TestSelectSystemTables(t *testing.T) {
	for _, test := range systemTableSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectQuery(t, test)
		})
	}
}

type testCommitClock struct {
	unixNano int64
}

func (tcc *testCommitClock) Now() time.Time {
	now := time.Unix(0, tcc.unixNano)
	tcc.unixNano += int64(time.Hour)
	return now
}

func installTestCommitClock() func() {
	oldNowFunc := datas.CommitNowFunc
	oldCommitLoc := datas.CommitLoc
	tcc := &testCommitClock{}
	datas.CommitNowFunc = tcc.Now
	datas.CommitLoc = time.UTC
	return func() {
		datas.CommitNowFunc = oldNowFunc
		datas.CommitLoc = oldCommitLoc
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testSelectQuery(t *testing.T, test SelectTest) {
	validateTest(t, test)

	cleanup := installTestCommitClock()
	defer cleanup()

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	actualRows, sch, err := executeSelect(t, context.Background(), dEnv, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		// Too much work to synchronize error messages between the two implementations, so for now we'll just assert that an error occurred.
		// require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	// JSON columns must be compared using like so
	assert.Equal(t, len(test.ExpectedRows), len(actualRows))
	for i := 0; i < len(test.ExpectedRows); i++ {
		assert.Equal(t, len(test.ExpectedRows[i]), len(actualRows[i]))
		for j := 0; j < len(test.ExpectedRows[i]); j++ {
			if _, ok := actualRows[i][j].(json.NomsJSON); ok {
				cmp, err := actualRows[i][j].(json.NomsJSON).Compare(nil, test.ExpectedRows[i][j].(json.NomsJSON))
				assert.NoError(t, err)
				assert.Equal(t, 0, cmp)
			} else {
				assert.Equal(t, test.ExpectedRows[i][j], actualRows[i][j])
			}

		}
	}

	var sqlSchema sql.Schema
	if test.ExpectedSqlSchema != nil {
		sqlSchema = test.ExpectedSqlSchema
	} else {
		sqlSchema = mustSqlSchema(test.ExpectedSchema)
	}

	assertSchemasEqual(t, sqlSchema, sch)
}

func testSelectDiffQuery(t *testing.T, test SelectTest) {
	validateTest(t, test)

	ctx := context.Background()

	cleanup := installTestCommitClock()
	defer cleanup()

	dEnv := dtestutils.CreateTestEnv()
	InitializeWithHistory(t, ctx, dEnv, CreateHistory(ctx, dEnv, t)...)
	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	cs, err := doltdb.NewCommitSpec("main")
	require.NoError(t, err)

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	require.NoError(t, err)

	root, err := cm.GetRootValue(ctx)
	require.NoError(t, err)

	err = dEnv.UpdateStagedRoot(ctx, root)
	require.NoError(t, err)

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	root, err = dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)

	root = UpdateTables(t, ctx, root, CreateWorkingRootUpdate())

	err = dEnv.UpdateWorkingRoot(ctx, root)
	require.NoError(t, err)

	actualRows, sch, err := executeSelect(t, ctx, dEnv, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedRows, actualRows)

	var sqlSchema sql.Schema
	if test.ExpectedSqlSchema != nil {
		sqlSchema = test.ExpectedSqlSchema
	} else {
		sqlSchema = mustSqlSchema(test.ExpectedSchema)
	}

	assertSchemasEqual(t, sqlSchema, sch)
}

func validateTest(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil && test.ExpectedSqlSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(test.ExpectedErr) == 0 && (test.ExpectedSchema == nil) == (test.ExpectedSqlSchema == nil) {
		require.Fail(t, "Incorrect test setup: must set at most one of ExpectedSchema, ExpectedSqlSchema")
	}

	if len(singleSelectQueryTest) > 0 && test.Name != singleSelectQueryTest {
		t.Skip("Skipping tests until " + singleSelectQueryTest)
	}

	if len(singleSelectQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenSelect {
		t.Skip("Skipping test broken on SQL engine")
	}
}
