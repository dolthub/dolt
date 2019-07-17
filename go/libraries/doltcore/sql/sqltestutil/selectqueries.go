package sqltestutil

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

// This file defines test queries and expected results. The purpose of defining them here is to make them portable --
// usable in multiple contexts as we implement SQL support.

// SetupFunc can be run to perform additional setup work before a test case
type SetupFn func(t *testing.T, dEnv *env.DoltEnv)

// Structure for a test of a select query
type SelectTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The query to run, excluding an ending semicolon
	Query string
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

//
// Collection of query tests for conformance and performance testing, grouped by categories.
//

// BasicSelectTests cover basic select statement features and error handling
var BasicSelectTests = []SelectTest{
	{
		Name:           "select * on primary key",
		Query:          "select * from people where id = 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select * ",
		Query:          "select * from people",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 1",
		Query:          "select * from people limit 1",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 1 offset 0",
		Query:          "select * from people limit 0,1",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 1 offset 1",
		Query:          "select * from people limit 1 offset 1;",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 1 offset 5",
		Query:          "select * from people limit 5,1",
		ExpectedRows:   CompressRows(PeopleTestSchema, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 1 offset 6",
		Query:          "select * from people limit 6,1",
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 0",
		Query:          "select * from people limit 0",
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, limit 0 offset 0",
		Query:          "select * from people limit 0,0",
		ExpectedRows:   Rs(),
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
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < int",
		Query:          "select * from people where age < 40",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < int, limit 1",
		Query:          "select * from people where age < 40 limit 1",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < int, limit 2",
		Query:          "select * from people where age < 40 limit 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < int, limit 100",
		Query:          "select * from people where age < 40 limit 100",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by int",
		Query:          "select * from people order by id",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by int desc",
		Query:          "select * from people order by id desc",
		ExpectedRows:   CompressRows(PeopleTestSchema, Barney, Moe, Lisa, Bart, Marge, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	// TODO: float logic seems broken in sql engine
	{
		Name:            "select *, order by float",
		Query:           "select * from people order by rating",
		ExpectedRows:    CompressRows(PeopleTestSchema, Barney, Moe, Marge, Homer, Bart, Lisa),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by string",
		Query:          "select * from people order by first",
		ExpectedRows:   CompressRows(PeopleTestSchema, Barney, Bart, Homer, Lisa, Marge, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by string,string",
		Query:          "select * from people order by last desc, first asc",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe, Bart, Homer, Lisa, Marge, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by with limit",
		Query:          "select * from people order by first limit 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Barney, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, order by string,string with limit",
		Query:          "select * from people order by last desc, first asc limit 2",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe, Bart),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where > int reversed",
		Query:          "select * from people where 40 > age",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where <= int",
		Query:          "select * from people where age <= 40",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where >= int reversed",
		Query:          "select * from people where 40 >= age",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where > int",
		Query:          "select * from people where age > 40",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < int reversed",
		Query:          "select * from people where 40 < age",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where >= int",
		Query:          "select * from people where age >= 40",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where <= int reversed",
		Query:          "select * from people where 40 <= age",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where > string",
		Query:          "select * from people where last > 'Simpson'",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < string",
		Query:          "select * from people where last < 'Simpson'",
		ExpectedRows:   CompressRows(PeopleTestSchema, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where = string",
		Query:          "select * from people where last = 'Simpson'",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, where > float",
		Query:           "select * from people where rating > 8.0 order by id",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer, Bart, Lisa),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where < float",
		Query:          "select * from people where rating < 8.0",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, where = float",
		Query:           "select * from people where rating = 8.0",
		ExpectedRows:    CompressRows(PeopleTestSchema, Marge),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, where < float reversed",
		Query:           "select * from people where 8.0 < rating",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer, Bart, Lisa),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where > float reversed",
		Query:          "select * from people where 8.0 > rating",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, where = float reversed",
		Query:           "select * from people where 8.0 = rating",
		ExpectedRows:    CompressRows(PeopleTestSchema, Marge),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where bool = ",
		Query:          "select * from people where is_married = true",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where bool = false ",
		Query:          "select * from people where is_married = false",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where bool <> ",
		Query:          "select * from people where is_married <> false",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, where bool",
		Query:          "select * from people where is_married",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, and clause",
		Query:          "select * from people where is_married and age > 38",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, or clause",
		Query:          "select * from people where is_married or age < 20",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, in clause string",
		Query:          "select * from people where first in ('Homer', 'Marge')",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, in clause integer",
		Query:          "select * from people where age in (-10, 40)",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, in clause float",
		Query:           "select * from people where rating in (-10.0, 8.5)",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, in clause, mixed types",
		Query:           "select * from people where first in ('Homer', 40)",
		ExpectedErr:     "Type mismatch: mixed types in list literal '('Homer', 40)'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, in clause, mixed numeric types",
		Query:           "select * from people where age in (-10.0, 40)",
		ExpectedErr:     "Type mismatch: mixed types in list literal '(-10.0, 40)'",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "select *, not in clause",
		Query:          "select * from people where first not in ('Homer', 'Marge')",
		ExpectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, in clause single element",
		Query:          "select * from people where first in ('Homer')",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, in clause single type mismatch",
		Query:           "select * from people where first in (1.0)",
		ExpectedErr:     "Type mismatch:",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "select *, is null clause ",
		Query:          "select * from people where uuid is null",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, is not null clause ",
		Query:          "select * from people where uuid is not null",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, is true clause ",
		Query:           "select * from people where is_married is true",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, is not true clause ",
		Query:           "select * from people where is_married is not true",
		ExpectedRows:    CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, is false clause ",
		Query:           "select * from people where is_married is false",
		ExpectedRows:    CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, is not false clause ",
		Query:           "select * from people where is_married is not false",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer, Marge),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, is true clause on non-bool column",
		Query:           "select * from people where age is true",
		ExpectedErr:     "Type mismatch:",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "binary expression in select",
		Query:          "select age + 1 as a from people where is_married order by a",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(39)), NewResultSetRow(types.Int(41))),
		ExpectedSchema: NewResultSetSchema("a", types.IntKind),
	},
	{
		Name:           "and expression in select",
		Query:          "select is_married and age >= 40 from people where last = 'Simpson' order by id limit 2",
		ExpectedRows:   Rs(NewResultSetRow(types.Bool(true)), NewResultSetRow(types.Bool(false))),
		ExpectedSchema: NewResultSetSchema("is_married and age >= 40", types.BoolKind),
	},
	{
		Name:  "or expression in select",
		Query: "select first, age <= 10 or age >= 40 as not_marge from people where last = 'Simpson' order by id desc",
		ExpectedRows: Rs(
			NewResultSetRow(types.String("Lisa"), types.Bool(true)),
			NewResultSetRow(types.String("Bart"), types.Bool(true)),
			NewResultSetRow(types.String("Marge"), types.Bool(false)),
			NewResultSetRow(types.String("Homer"), types.Bool(true)),
		),
		ExpectedSchema: NewResultSetSchema("first", types.StringKind, "not_marge", types.BoolKind),
	},
	{
		Name:           "unary expression in select",
		Query:          "select -age as age from people where is_married order by age",
		ExpectedRows:   Rs(NewResultSetRow(types.Int(-40)), NewResultSetRow(types.Int(-38))),
		ExpectedSchema: NewResultSetSchema("age", types.IntKind),
	},
	{
		Name:            "unary expression in select, alias named after column",
		Query:           "select -age as age from people where is_married order by people.age",
		ExpectedRows:    Rs(NewResultSetRow(types.Int(-38)), NewResultSetRow(types.Int(-40))),
		ExpectedSchema:  NewResultSetSchema("age", types.IntKind),
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, -column",
		Query:           "select * from people where -rating = -8.5",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, -column, string type",
		Query:           "select * from people where -first = 'Homer'",
		ExpectedErr:     "Unsupported type for unary - operation: varchar",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "select *, binary + in where",
		Query:          "select * from people where age + 1 = 41",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, binary - in where",
		Query:          "select * from people where age - 1 = 39",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, binary / in where",
		Query:          "select * from people where age / 2 = 20",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, binary * in where",
		Query:          "select * from people where age * 2 = 80",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select *, binary % in where",
		Query:          "select * from people where age % 4 = 0",
		ExpectedRows:   CompressRows(PeopleTestSchema, Homer, Lisa, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:            "select *, complex binary expr in where",
		Query:           "select * from people where age / 4 + 2 * 2 = 14",
		ExpectedRows:    CompressRows(PeopleTestSchema, Homer, Barney),
		ExpectedSchema:  CompressSchema(PeopleTestSchema),
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, binary + in where type mismatch",
		Query:           "select * from people where first + 1 = 41",
		ExpectedErr:     "Type mismatch evaluating expression 'first + 1'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, binary - in where type mismatch",
		Query:           "select * from people where first - 1 = 39",
		ExpectedErr:     "Type mismatch evaluating expression 'first - 1'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, binary / in where type mismatch",
		Query:           "select * from people where first / 2 = 20",
		ExpectedErr:     "Type mismatch evaluating expression 'first / 2'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, binary * in where type mismatch",
		Query:           "select * from people where first * 2 = 80",
		ExpectedErr:     "Type mismatch evaluating expression 'first * 2'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "select *, binary % in where type mismatch",
		Query:           "select * from people where first % 4 = 0",
		ExpectedErr:     "Type mismatch evaluating expression 'first % 4'",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "select * with where, order by",
		Query:          "select * from people where `uuid` is not null and first <> 'Marge' order by last desc, age",
		ExpectedRows:   CompressRows(PeopleTestSchema, Moe, Lisa, Bart, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "select subset of cols",
		Query:          "select first, last from people where age >= 40",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
		ExpectedSchema: CompressSchema(PeopleTestSchema, "first", "last"),
	},
	{
		Name:           "column aliases",
		Query:          "select first as f, last as l from people where age >= 40",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
		ExpectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
	},
	{
		Name:           "duplicate column aliases",
		Query:          "select first as f, last as f from people where age >= 40",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
		ExpectedSchema: NewResultSetSchema("f", types.StringKind, "f", types.StringKind),
	},
	{
		Name:  "column selected more than once",
		Query: "select first, first from people where age >= 40 order by id",
		ExpectedRows: Rs(
			NewResultSetRow(types.String("Homer"), types.String("Homer")),
			NewResultSetRow(types.String("Moe"), types.String("Moe")),
			NewResultSetRow(types.String("Barney"), types.String("Barney")),
		),
		ExpectedSchema: NewResultSetSchema("first", types.StringKind, "first", types.StringKind),
	},

	// TODO: fix this. To make this work we need to track selected tables along with their aliases. It's not an error to
	//  select the same table multiple times, as long as each occurrence has a unique name
	// {
	// 	name:        "duplicate table selection",
	// 	query:       "select first as f, last as f from people, people where age >= 40",
	// 	expectedErr: "Non-unique table name / alias: 'people'",
	// },
	{
		Name:            "duplicate table alias",
		Query:           "select * from people p, people p where age >= 40",
		ExpectedErr:     "Non-unique table name / alias: 'p'",
		SkipOnSqlEngine: true,
	},
	{
		Name:            "column aliases in where clause",
		Query:           `select first as f, last as l from people where f = "Homer"`,
		ExpectedErr:     "Unknown column: 'f'",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "select subset of columns with order by",
		Query:          "select first from people order by age, first",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first"), Lisa, Bart, Marge, Barney, Homer, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema, "first"),
	},
	{
		Name:           "column aliases with order by",
		Query:          "select first as f from people order by age, f",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first"), Lisa, Bart, Marge, Barney, Homer, Moe),
		ExpectedSchema: NewResultSetSchema("f", types.StringKind),
	},
	{
		Name:            "ambiguous column in order by",
		Query:           "select first as f, last as f from people order by f",
		ExpectedErr:     "Ambiguous column: 'f'",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "table aliases",
		Query:          "select p.first as f, people.last as l from people p where p.first = 'Homer'",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer),
		ExpectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
	},
	{
		Name:           "table aliases without column aliases",
		Query:          "select p.first, people.last from people p where p.first = 'Homer'",
		ExpectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer),
		ExpectedSchema: NewResultSetSchema("first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:        "table aliases with bad alias",
		Query:       "select m.first as f, p.last as l from people p where p.f = 'Homer'",
		ExpectedErr: "Unknown table: 'm'",
	},
	{
		Name: "column aliases, all columns",
		Query: `select id as i, first as f, last as l, is_married as m, age as a,
				rating as r, uuid as u, num_episodes as n from people
				where age >= 40`,
		ExpectedRows: CompressRows(PeopleTestSchema, Homer, Moe, Barney),
		ExpectedSchema: NewResultSetSchema("i", types.IntKind, "f", types.StringKind,
			"l", types.StringKind, "m", types.BoolKind, "a", types.IntKind, "r", types.FloatKind,
			"u", types.UUIDKind, "n", types.UintKind),
	},
	{
		Name:           "select *, not equals",
		Query:          "select * from people where age <> 40",
		ExpectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa, Moe),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "empty result set",
		Query:          "select * from people where age > 80",
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema),
	},
	{
		Name:           "empty result set with columns",
		Query:          "select id, age from people where age > 80",
		ExpectedRows:   Rs(),
		ExpectedSchema: CompressSchema(PeopleTestSchema, "id", "age"),
	},
	{
		Name:        "unknown table",
		Query:       "select * from dne",
		ExpectedErr: `Unknown table: 'dne'`,
	},
	{
		Name:        "unknown table in join",
		Query:       "select * from people join dne",
		ExpectedErr: `Unknown table: 'dne'`,
	},
	{
		Name:            "no table",
		Query:           "select 1",
		ExpectedErr:     `Selects without a table are not supported:`,
		SkipOnSqlEngine: true, // not actually an error, just not supported by our implementation
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
		Query:       "select * from people where function(first)",
		ExpectedErr: "not supported",
	},
	{
		Name:            "type mismatch in where clause",
		Query:           `select * from people where id = "0"`,
		ExpectedErr:     "Type mismatch:",
		SkipOnSqlEngine: true,
	},
}

// SQL is supposed to be case insensitive. These are tests of that promise.
var CaseSensitivityTests = []SelectTest {
	{
		Name:           "table name has mixed case, select lower case",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select test from mixedcase",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:           "table name has mixed case, select upper case",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select test from MIXEDCASE",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:           "qualified select *",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select mixedcAse.* from MIXEDCASE",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		SkipOnSqlEngine: true,
	},
	{
		Name:           "qualified select column",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select mixedcAse.TeSt from MIXEDCASE",
		ExpectedSchema: NewResultSetSchema("TeSt", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:           "table alias select *",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select Mc.* from MIXEDCASE as mc",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		SkipOnSqlEngine: true,
	},
	{
		Name:           "table alias select column",
		AdditionalSetup: CreateTableFn("MiXeDcAsE",
			NewSchema("test", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select mC.TeSt from MIXEDCASE as MC",
		ExpectedSchema: NewResultSetSchema("TeSt", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:        "multiple tables with the same case-insensitive name, exact match",
		AdditionalSetup: Compose(
			CreateTableFn("tableName", NewSchema("test", types.StringKind), NewRow(types.String("1"))),
			CreateTableFn("TABLENAME", NewSchema("test", types.StringKind)),
			CreateTableFn("tablename", NewSchema("test", types.StringKind)),
		),
		Query:          "select test from tableName",
		ExpectedSchema: NewResultSetSchema("test", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		SkipOnSqlEngine: true,
	},
	{
		Name:        "multiple tables with the same case-insensitive name, no exact match",
		AdditionalSetup: Compose(
			CreateTableFn("tableName", NewSchema("test", types.StringKind)),
			CreateTableFn("TABLENAME", NewSchema("test", types.StringKind)),
		),
		Query:       "select test from tablename",
		ExpectedErr: "Ambiguous table: 'tablename'",
		SkipOnSqlEngine: true,
	},
	{
		Name:        "alias with same name as table",
		AdditionalSetup: Compose(
			CreateTableFn("tableName", NewSchema("test", types.StringKind)),
			CreateTableFn("other", NewSchema("othercol", types.StringKind)),
		),
		Query:       "select other.test from tablename as other, other",
		ExpectedErr: "Non-unique table name / alias: 'other'",
		SkipOnSqlEngine: true,
	},
	{
		Name:        "two table aliases with same name",
		AdditionalSetup: Compose(
			CreateTableFn("tableName", NewSchema("test", types.StringKind)),
			CreateTableFn("other", NewSchema("othercol", types.StringKind)),
		),
		Query:       "select bad.test from tablename as bad, other as bad",
		ExpectedErr: "Non-unique table name / alias: 'bad'",
		SkipOnSqlEngine: true,
	},
	{
		Name:           "column name has mixed case, select lower case",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select mixedcase from test",
		ExpectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:           "column name has mixed case, select upper case",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select MIXEDCASE from test",
		ExpectedSchema: NewResultSetSchema("MIXEDCASE", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:           "select with multiple matching columns, exact match",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			NewRow(types.String("1"), types.String("2"))),
		Query:          "select mixedcase from test",
		ExpectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("2"))),
	},
	{
		Name:           "select with multiple matching columns, exact match #2",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			NewRow(types.String("1"), types.String("2"))),
		Query:          "select MiXeDcAsE from test",
		ExpectedSchema: NewResultSetSchema("MiXeDcAsE", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		SkipOnSqlEngine: true,
	},
	{
		Name:        "select with multiple matching columns, no exact match",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			NewRow(types.String("1"), types.String("2"))),
		Query:       "select MIXEDCASE from test",
		ExpectedErr: "Ambiguous column: 'MIXEDCASE'",
		SkipOnSqlEngine: true,
	},
	{
		Name:        "select with multiple matching columns, no exact match, table alias",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			NewRow(types.String("1"), types.String("2"))),
		Query:       "select t.MIXEDCASE from test t",
		ExpectedErr: "Ambiguous column: 'MIXEDCASE'",
		SkipOnSqlEngine: true,
	},
	// TODO: this could be handled better (not change the case of the result set schema), but the parser will silently
	//  lower-case any column name expression that is a reserved word. Changing that is harder.
	{
		Name: "column is reserved word, select not backticked",
		AdditionalSetup: CreateTableFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
		Query:          "select Timestamp from test",
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		ExpectedSchema: NewResultSetSchema("timestamp", types.StringKind),
	},
	{
		Name:      "column is reserved word, qualified with table alias",
		AdditionalSetup: CreateTableFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
		Query:          "select t.Timestamp from test as t",
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		ExpectedSchema: NewResultSetSchema("timestamp", types.StringKind),
	},
	{
		Name:      "column is reserved word, select not backticked #2",
		AdditionalSetup: CreateTableFn("test",
			NewSchema("YeAr", types.StringKind),
			NewRow(types.String("1"))),
		Query:          "select Year from test",
		ExpectedSchema: NewResultSetSchema("year", types.StringKind),
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
	},
	{
		Name:      "column is reserved word, select backticked",
		AdditionalSetup: CreateTableFn("test",
			NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
		Query:          "select `Timestamp` from test",
		ExpectedRows:   Rs(NewResultSetRow(types.String("1"))),
		ExpectedSchema: NewResultSetSchema("Timestamp", types.StringKind),
	},
	{
		Name:      "column is reserved word, select backticked #2",
		AdditionalSetup: CreateTableFn("test",
			NewSchema(
				"Year", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
		Query:       "select `Year`, `OR`, `SELect`, `anD` from test",
		ExpectedSchema: NewResultSetSchema(
			"Year", types.StringKind,
			"OR", types.StringKind,
			"SELect", types.StringKind,
			"anD", types.StringKind),
		ExpectedRows: Rs(NewResultSetRow(types.String("1"), types.String("aaa"), types.String("create"), types.String("1.1"))),
	},
}

// Tests of join functionality, basically any query involving more than one table should go here for now.
var JoinTests = []SelectTest{
	{
		Name:  "Full cross product",
		Query: `select * from people, episodes`,
		ExpectedRows: Rs(
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
		ExpectedRows: Rs(
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
		ExpectedRows: Rs(
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
		Name:  "ambiguous column in select",
		Query: `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
		ExpectedErr: "Ambiguous column: 'id'",
	},
	{
		Name:  "ambiguous column in where",
		Query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
		ExpectedErr: "Ambiguous column: 'id'",
	},
	{
		Name:  "Natural join with where clause, select subset of columns",
		Query: `select e.id, p.id, e.name, p.first, p.last from people p, episodes e where e.id = p.id`,
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with where clause and column aliases",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with where clause and quoted column alias",
		Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
			"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with join clause",
		Query: `select * from people p join episodes e on e.id = p.id`,
		ExpectedRows: Rs(
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
		ExpectedRows: Rs(
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
		Query: `select e.id, p.id, e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with join clause, select subset of columns, join columns not selected",
		Query: `select e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
		ExpectedRows: Rs(
			NewResultSetRow(types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name: "Natural join with join clause, select subset of columns, order by clause",
		Query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by e.name`,
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name: "Natural join with join clause, select subset of columns, order by clause on non-selected column",
		Query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by age`,
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
			"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and column aliases",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and column aliases, order by",
		Query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id order by ename",
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
			"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Natural join with join clause and quoted column alias",
		Query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
		ExpectedRows: Rs(
			NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
			NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
			NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
			NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
		),
		ExpectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
			"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
	},
	{
		Name:  "Join from table with two key columns to table with one key column",
		Query: `select a.episode_id as eid, p.id as pid, p.first
						from appearances a join people p on a.character_id = p.id order by eid, pid`,
		ExpectedRows: Rs(
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
			"first", types.StringKind),
	},
}

// CreateTableFn returns a SetupFunc that creates a table with the rows given
func CreateTableFn(tableName string, tableSchema schema.Schema, initialRows ...row.Row) SetupFn {
	return func(t *testing.T, dEnv *env.DoltEnv) {
		dtestutils.CreateTestTable(t, dEnv, tableName, tableSchema, initialRows...)
	}
}

// Compose takes an arbitrary number of SetupFns and composes them into a single func which executes all funcs given.
func Compose(fns ...SetupFn) SetupFn {
	return func(t *testing.T, dEnv *env.DoltEnv) {
		for _, f := range fns {
			f(t, dEnv)
		}
	}
}