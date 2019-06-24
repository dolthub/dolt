package sql

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/xwb1989/sqlparser"
)

func TestExecuteSelect(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:           "select * ",
			query:          "select * from people",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 1",
			query:          "select * from people limit 1",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 0",
			query:          "select * from people limit 0,1",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 1",
			query:          "select * from people limit 1,1",
			expectedRows:   CompressRows(PeopleTestSchema, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 5",
			query:          "select * from people limit 5,1",
			expectedRows:   CompressRows(PeopleTestSchema, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 6",
			query:          "select * from people limit 6,1",
			expectedRows:   Rs(),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 0",
			query:          "select * from people limit 0",
			expectedRows:   Rs(),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, limit 0 offset 0",
			query:          "select * from people limit 0,0",
			expectedRows:   Rs(),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, limit -1",
			query:       "select * from people limit -1",
			expectedErr: "Limit must be >= 0 if supplied",
		},
		{
			name:        "select *, offset -1",
			query:       "select * from people limit -1,1",
			expectedErr: "Offset must be >= 0 if supplied",
		},
		{
			name:           "select *, limit 100",
			query:          "select * from people limit 100",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < int",
			query:          "select * from people where age < 40",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 1",
			query:          "select * from people where age < 40 limit 1",
			expectedRows:   CompressRows(PeopleTestSchema, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 2",
			query:          "select * from people where age < 40 limit 2",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 100",
			query:          "select * from people where age < 40 limit 100",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by int",
			query:          "select * from people order by id",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by int desc",
			query:          "select * from people order by id desc",
			expectedRows:   CompressRows(PeopleTestSchema, Barney, Moe, Lisa, Bart, Marge, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by float",
			query:          "select * from people order by rating",
			expectedRows:   CompressRows(PeopleTestSchema, Barney, Moe, Marge, Homer, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by string",
			query:          "select * from people order by first",
			expectedRows:   CompressRows(PeopleTestSchema, Barney, Bart, Homer, Lisa, Marge, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by string,string",
			query:          "select * from people order by last desc, first asc",
			expectedRows:   CompressRows(PeopleTestSchema, Moe, Bart, Homer, Lisa, Marge, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by with limit",
			query:          "select * from people order by first limit 2",
			expectedRows:   CompressRows(PeopleTestSchema, Barney, Bart),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, order by string,string with limit",
			query:          "select * from people order by last desc, first asc limit 2",
			expectedRows:   CompressRows(PeopleTestSchema, Moe, Bart),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where > int reversed",
			query:          "select * from people where 40 > age",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where <= int",
			query:          "select * from people where age <= 40",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where >= int reversed",
			query:          "select * from people where 40 >= age",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where > int",
			query:          "select * from people where age > 40",
			expectedRows:   CompressRows(PeopleTestSchema, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < int reversed",
			query:          "select * from people where 40 < age",
			expectedRows:   CompressRows(PeopleTestSchema, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where >= int",
			query:          "select * from people where age >= 40",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where <= int reversed",
			query:          "select * from people where 40 <= age",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where > string",
			query:          "select * from people where last > 'Simpson'",
			expectedRows:   CompressRows(PeopleTestSchema, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < string",
			query:          "select * from people where last < 'Simpson'",
			expectedRows:   CompressRows(PeopleTestSchema, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where = string",
			query:          "select * from people where last = 'Simpson'",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where > float",
			query:          "select * from people where rating > 8.0",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < float",
			query:          "select * from people where rating < 8.0",
			expectedRows:   CompressRows(PeopleTestSchema, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where = float",
			query:          "select * from people where rating = 8.0",
			expectedRows:   CompressRows(PeopleTestSchema, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where < float reversed",
			query:          "select * from people where 8.0 < rating",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where > float reversed",
			query:          "select * from people where 8.0 > rating",
			expectedRows:   CompressRows(PeopleTestSchema, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where = float reversed",
			query:          "select * from people where 8.0 = rating",
			expectedRows:   CompressRows(PeopleTestSchema, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where bool = ",
			query:          "select * from people where is_married = true",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where bool = false ",
			query:          "select * from people where is_married = false",
			expectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where bool <> ",
			query:          "select * from people where is_married <> false",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, where bool",
			query:          "select * from people where is_married",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, and clause",
			query:          "select * from people where is_married and age > 38",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, or clause",
			query:          "select * from people where is_married or age < 20",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge, Bart, Lisa),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, in clause string",
			query:          "select * from people where first in ('Homer', 'Marge')",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, in clause integer",
			query:          "select * from people where age in (-10, 40)",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, in clause float",
			query:          "select * from people where rating in (-10.0, 8.5)",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, in clause, mixed types",
			query:       "select * from people where first in ('Homer', 40)",
			expectedErr: "Type mismatch: mixed types in list literal '('Homer', 40)'",
		},
		{
			name:        "select *, in clause, mixed numeric types",
			query:       "select * from people where age in (-10.0, 40)",
			expectedErr: "Type mismatch: mixed types in list literal '(-10.0, 40)'",
		},
		{
			name:           "select *, not in clause",
			query:          "select * from people where first not in ('Homer', 'Marge')",
			expectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, in clause single element",
			query:          "select * from people where first in ('Homer')",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, in clause single type mismatch",
			query:       "select * from people where first in (1.0)",
			expectedErr: "Type mismatch:",
		},
		{
			name:           "select *, is null clause ",
			query:          "select * from people where uuid is null",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, is not null clause ",
			query:          "select * from people where uuid is not null",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, is true clause ",
			query:          "select * from people where is_married is true",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, is not true clause ",
			query:          "select * from people where is_married is not true",
			expectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, is false clause ",
			query:          "select * from people where is_married is false",
			expectedRows:   CompressRows(PeopleTestSchema, Bart, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, is not false clause ",
			query:          "select * from people where is_married is not false",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Marge),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, is true clause on non-bool column",
			query:       "select * from people where age is true",
			expectedErr: "Type mismatch:",
		},
		{
			name:           "binary expression in select",
			query:          "select age + 1 as a from people where is_married order by a",
			expectedRows:   Rs(NewResultSetRow(types.Int(39)), NewResultSetRow(types.Int(41))),
			expectedSchema: NewResultSetSchema("a", types.IntKind),
		},
		{
			name:           "and expression in select",
			query:          "select is_married and age >= 40 from people where last = 'Simpson' order by id limit 2",
			expectedRows:   Rs(NewResultSetRow(types.Bool(true)), NewResultSetRow(types.Bool(false))),
			expectedSchema: NewResultSetSchema("is_married and age >= 40", types.BoolKind),
		},
		{
			name:  "or expression in select",
			query: "select first, age <= 10 or age >= 40 as not_marge from people where last = 'Simpson' order by id desc",
			expectedRows: Rs(
				NewResultSetRow(types.String("Lisa"), types.Bool(true)),
				NewResultSetRow(types.String("Bart"), types.Bool(true)),
				NewResultSetRow(types.String("Marge"), types.Bool(false)),
				NewResultSetRow(types.String("Homer"), types.Bool(true)),
			),
			expectedSchema: NewResultSetSchema("first", types.StringKind, "not_marge", types.BoolKind),
		},
		{
			name:           "unary expression in select",
			query:          "select -age as age from people where is_married order by age",
			expectedRows:   Rs(NewResultSetRow(types.Int(-40)), NewResultSetRow(types.Int(-38))),
			expectedSchema: NewResultSetSchema("age", types.IntKind),
		},
		{
			name:           "unary expression in select, alias named after column",
			query:          "select -age as age from people where is_married order by people.age",
			expectedRows:   Rs(NewResultSetRow(types.Int(-38)), NewResultSetRow(types.Int(-40))),
			expectedSchema: NewResultSetSchema("age", types.IntKind),
		},
		{
			name:           "select *, -column",
			query:          "select * from people where -rating = -8.5",
			expectedRows:   CompressRows(PeopleTestSchema, Homer),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, -column, string type",
			query:       "select * from people where -first = 'Homer'",
			expectedErr: "Unsupported type for unary - operation: varchar",
		},
		{
			name:           "select *, binary + in where",
			query:          "select * from people where age + 1 = 41",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, binary - in where",
			query:          "select * from people where age - 1 = 39",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, binary / in where",
			query:          "select * from people where age / 2 = 20",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, binary * in where",
			query:          "select * from people where age * 2 = 80",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, binary % in where",
			query:          "select * from people where age % 4 = 0",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Lisa, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select *, complex binary expr in where",
			query:          "select * from people where age / 4 + 2 * 2 = 14",
			expectedRows:   CompressRows(PeopleTestSchema, Homer, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:        "select *, binary + in where type mismatch",
			query:       "select * from people where first + 1 = 41",
			expectedErr: "Type mismatch evaluating expression 'first + 1'",
		},
		{
			name:        "select *, binary - in where type mismatch",
			query:       "select * from people where first - 1 = 39",
			expectedErr: "Type mismatch evaluating expression 'first - 1'",
		},
		{
			name:        "select *, binary / in where type mismatch",
			query:       "select * from people where first / 2 = 20",
			expectedErr: "Type mismatch evaluating expression 'first / 2'",
		},
		{
			name:        "select *, binary * in where type mismatch",
			query:       "select * from people where first * 2 = 80",
			expectedErr: "Type mismatch evaluating expression 'first * 2'",
		},
		{
			name:        "select *, binary % in where type mismatch",
			query:       "select * from people where first % 4 = 0",
			expectedErr: "Type mismatch evaluating expression 'first % 4'",
		},
		{
			name:           "select * with where, order by",
			query:          "select * from people where `uuid` is not null and first <> 'Marge' order by last desc, age",
			expectedRows:   CompressRows(PeopleTestSchema, Moe, Lisa, Bart, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "select subset of cols",
			query:          "select first, last from people where age >= 40",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
			expectedSchema: CompressSchema(PeopleTestSchema, "first", "last"),
		},
		{
			name:           "column aliases",
			query:          "select first as f, last as l from people where age >= 40",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
			expectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:           "duplicate column aliases",
			query:          "select first as f, last as f from people where age >= 40",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer, Moe, Barney),
			expectedSchema: NewResultSetSchema("f", types.StringKind, "f", types.StringKind),
		},
		{
			name:           "column selected more than once",
			query:          "select first, first from people where age >= 40 order by id",
			expectedRows:   Rs(
				NewResultSetRow(types.String("Homer"), types.String("Homer")),
				NewResultSetRow(types.String("Moe"), types.String("Moe")),
				NewResultSetRow(types.String("Barney"), types.String("Barney")),
			),
			expectedSchema: NewResultSetSchema("first", types.StringKind, "first", types.StringKind),
		},

		// TODO: fix this. To make this work we need to track selected tables along with their aliases. It's not an error to
		//  select the same table multiple times, as long as each occurrence has a unique name
		// {
		// 	name:        "duplicate table selection",
		// 	query:       "select first as f, last as f from people, people where age >= 40",
		// 	expectedErr: "Non-unique table name / alias: 'people'",
		// },
		{
			name:        "duplicate table alias",
			query:       "select * from people p, people p where age >= 40",
			expectedErr: "Non-unique table name / alias: 'p'",
		},
		{
			name:        "column aliases in where clause",
			query:       `select first as f, last as l from people where f = "Homer"`,
			expectedErr: "Unknown column: 'f'",
		},
		{
			name:           "select subset of columns with order by",
			query:          "select first from people order by age, first",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first"), Lisa, Bart, Marge, Barney, Homer, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema, "first"),
		},
		{
			name:           "column aliases with order by",
			query:          "select first as f from people order by age, f",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first"), Lisa, Bart, Marge, Barney, Homer, Moe),
			expectedSchema: NewResultSetSchema("f", types.StringKind),
		},
		{
			name:        "ambiguous column in order by",
			query:       "select first as f, last as f from people order by f",
			expectedErr: "Ambiguous column: 'f'",
		},
		{
			name:           "table aliases",
			query:          "select p.first as f, people.last as l from people p where p.first = 'Homer'",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer),
			expectedSchema: NewResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:           "table aliases without column aliases",
			query:          "select p.first, people.last from people p where p.first = 'Homer'",
			expectedRows:   CompressRows(resultset.SubsetSchema(PeopleTestSchema, "first", "last"), Homer),
			expectedSchema: NewResultSetSchema("first", types.StringKind, "last", types.StringKind),
		},
		{
			name:        "table aliases with bad alias",
			query:       "select m.first as f, p.last as l from people p where p.f = 'Homer'",
			expectedErr: "Unknown table: 'm'",
		},
		{
			name: "column aliases, all columns",
			query: `select id as i, first as f, last as l, is_married as m, age as a,
				rating as r, uuid as u, num_episodes as n from people
				where age >= 40`,
			expectedRows: CompressRows(PeopleTestSchema, Homer, Moe, Barney),
			expectedSchema: NewResultSetSchema("i", types.IntKind, "f", types.StringKind,
				"l", types.StringKind, "m", types.BoolKind, "a", types.IntKind, "r", types.FloatKind,
				"u", types.UUIDKind, "n", types.UintKind),
		},
		{
			name:           "select *, not equals",
			query:          "select * from people where age <> 40",
			expectedRows:   CompressRows(PeopleTestSchema, Marge, Bart, Lisa, Moe),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "empty result set",
			query:          "select * from people where age > 80",
			expectedRows:   Rs(),
			expectedSchema: CompressSchema(PeopleTestSchema),
		},
		{
			name:           "empty result set with columns",
			query:          "select id, age from people where age > 80",
			expectedRows:   Rs(),
			expectedSchema: CompressSchema(PeopleTestSchema, "id", "age"),
		},
		{
			name:        "unknown table",
			query:       "select * from dne",
			expectedErr: `Unknown table: 'dne'`,
		},
		{
			name:        "unknown table in join",
			query:       "select * from people join dne",
			expectedErr: `Unknown table: 'dne'`,
		},
		{
			name:        "no table",
			query:       "select 1",
			expectedErr: `Selects without a table are not supported:`,
		},
		{
			name:        "unknown column in where",
			query:       "select * from people where dne > 8.0",
			expectedErr: `Unknown column: 'dne'`,
		},
		{
			name:        "unknown column in order by",
			query:       "select * from people where rating > 8.0 order by dne",
			expectedErr: `Unknown column: 'dne'`,
		},
		{
			name:        "unsupported comparison",
			query:       "select * from people where function(first)",
			expectedErr: "not supported",
		},
		{
			name:        "type mismatch in where clause",
			query:       `select * from people where id = "0"`,
			expectedErr: "Type mismatch:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, err := sqlparser.Parse(tt.query)
			if err != nil {
				assert.FailNow(t, "Couldn't parse query "+tt.query, "%v", err.Error())
			}

			s := sqlStatement.(*sqlparser.Select)

			if tt.expectedRows != nil && tt.expectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s)

			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}

func TestJoins(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:  "Full cross product",
			query: `select * from people, episodes`,
			expectedRows: Rs(
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
			expectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
		},
		{
			name:  "Natural join with where clause",
			query: `select * from people p, episodes e where e.id = p.id`,
			expectedRows: Rs(
				ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
				ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
				ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
				ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep4),
			),
			expectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
		},
		{
			name:  "Three table natural join with where clause",
			query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
			expectedRows: Rs(
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
			expectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
		},
		{
			name:        "ambiguous column in select",
			query:       `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
			expectedErr: "Ambiguous column: 'id'",
		},
		{
			name:        "ambiguous column in where",
			query:       `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
			expectedErr: "Ambiguous column: 'id'",
		},
		{
			name:  "Natural join with where clause, select subset of columns",
			query: `select e.id, p.id, e.name, p.first, p.last from people p, episodes e where e.id = p.id`,
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with where clause and column aliases",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with where clause and quoted column alias",
			query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause",
			query: `select * from people p join episodes e on e.id = p.id`,
			expectedRows: Rs(
				ConcatRows(PeopleTestSchema, Marge, EpisodesTestSchema, Ep1),
				ConcatRows(PeopleTestSchema, Bart, EpisodesTestSchema, Ep2),
				ConcatRows(PeopleTestSchema, Lisa, EpisodesTestSchema, Ep3),
				ConcatRows(PeopleTestSchema, Moe, EpisodesTestSchema, Ep4),
			),
			expectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
		},
		{
			name:  "Three table natural join with join clause",
			query: `select p.*, e.* from people p join appearances a on a.character_id = p.id join episodes e on a.episode_id = e.id`,
			expectedRows: Rs(
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
			expectedSchema: CompressSchemas(PeopleTestSchema, EpisodesTestSchema),
		},
		{
			name:  "Natural join with join clause, select subset of columns",
			query: `select e.id, p.id, e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause, select subset of columns, join columns not selected",
			query: `select e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
			expectedRows: Rs(
				NewResultSetRow(types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name: "Natural join with join clause, select subset of columns, order by clause",
			query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by e.name`,
			expectedRows: Rs(
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name: "Natural join with join clause, select subset of columns, order by clause on non-selected column",
			query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by age`,
			expectedRows: Rs(
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and column aliases",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and column aliases, order by",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id order by ename",
			expectedRows: Rs(
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and quoted column alias",
			query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
			expectedRows: Rs(
				NewResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				NewResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				NewResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				NewResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: NewResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Select)

			if tt.expectedRows != nil && tt.expectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s)

			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}

// Tests of case sensitivity handling
func TestCaseSensitivity(t *testing.T) {
	tests := []struct {
		name            string
		tableName       string
		tableSchema     schema.Schema
		initialRows     []row.Row
		additionalSetup func(t *testing.T, dEnv *env.DoltEnv)
		query           string
		expectedRows    []row.Row
		expectedSchema  schema.Schema
		expectedErr     string
	}{
		{
			name:           "table name has mixed case, select lower case",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select test from mixedcase",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "table name has mixed case, select upper case",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select test from MIXEDCASE",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "qualified select *",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcAse.* from MIXEDCASE",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "qualified select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcAse.TeSt from MIXEDCASE",
			expectedSchema: NewResultSetSchema("TeSt", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "table alias select *",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select Mc.* from MIXEDCASE as mc",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "table alias select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mC.TeSt from MIXEDCASE as MC",
			expectedSchema: NewResultSetSchema("TeSt", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:        "multiple tables with the same case-insensitive name, exact match",
			tableName:   "tableName",
			tableSchema: NewSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "TABLENAME", NewSchema("test", types.StringKind))
				dtestutils.CreateTestTable(t, dEnv, "tablename", NewSchema("test", types.StringKind))
			},
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select test from tableName",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:        "multiple tables with the same case-insensitive name, no exact match",
			tableName:   "tableName",
			tableSchema: NewSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "TABLENAME", NewSchema("test", types.StringKind))
			},
			initialRows: Rs(NewRow(types.String("1"))),
			query:       "select test from tablename",
			expectedErr: "Ambiguous table: 'tablename'",
		},
		{
			name:        "alias with same name as table",
			tableName:   "tableName",
			tableSchema: NewSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "other", NewSchema("othercol", types.StringKind))
			},
			initialRows: Rs(NewRow(types.String("1"))),
			query:       "select other.test from tablename as other, other",
			expectedErr: "Non-unique table name / alias: 'other'",
		},
		{
			name:        "two table aliases with same name",
			tableName:   "tableName",
			tableSchema: NewSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "other", NewSchema("othercol", types.StringKind))
			},
			initialRows: Rs(NewRow(types.String("1"))),
			query:       "select bad.test from tablename as bad, other as bad",
			expectedErr: "Non-unique table name / alias: 'bad'",
		},
		{
			name:           "column name has mixed case, select lower case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "column name has mixed case, select upper case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select MIXEDCASE from test",
			expectedSchema: NewResultSetSchema("MIXEDCASE", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "select uses incorrect case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:           "select with multiple matching columns, exact match",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"), types.String("2"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("2"))),
		},
		{
			name:           "select with multiple matching columns, exact case #2",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"), types.String("2"))),
			query:          "select MiXeDcAsE from test",
			expectedSchema: NewResultSetSchema("MiXeDcAsE", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:        "select with multiple matching columns, no exact match",
			tableName:   "test",
			tableSchema: NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows: Rs(NewRow(types.String("1"), types.String("2"))),
			query:       "select MIXEDCASE from test",
			expectedErr: "Ambiguous column: 'MIXEDCASE'",
		},
		{
			name:        "select with multiple matching columns, no exact match, table alias",
			tableName:   "test",
			tableSchema: NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows: Rs(NewRow(types.String("1"), types.String("2"))),
			query:       "select t.MIXEDCASE from test t",
			expectedErr: "Ambiguous column: 'MIXEDCASE'",
		},
		// TODO: this could be handled better (not change the case of the result set schema), but the parser will silently
		//  lower-case any column name expression that is a reserved word. Changing that is harder.
		{
			name:      "column is reserved word, select not backticked",
			tableName: "test",
			tableSchema: NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: Rs(
				NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select Timestamp from test",
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
			expectedSchema: NewResultSetSchema("timestamp", types.StringKind),
		},
		{
			name:      "column is reserved word, qualified with table alias",
			tableName: "test",
			tableSchema: NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: Rs(
				NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select t.Timestamp from test as t",
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
			expectedSchema: NewResultSetSchema("timestamp", types.StringKind),
		},
		{
			name:      "column is reserved word, select not backticked #2",
			tableName: "test",
			tableSchema: NewSchema(
				"YeAr", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select Year from test",
			expectedSchema: NewResultSetSchema("year", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			name:      "column is reserved word, select backticked",
			tableName: "test",
			tableSchema: NewSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: Rs(
				NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select `Timestamp` from test",
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
			expectedSchema: NewResultSetSchema("Timestamp", types.StringKind),
		},
		{
			name:      "column is reserved word, select backticked #2",
			tableName: "test",
			tableSchema: NewSchema(
				"Year", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: Rs(NewRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
			query:       "select `Year`, `OR`, `SELect`, `anD` from test",
			expectedSchema: NewResultSetSchema(
				"Year", types.StringKind,
				"OR", types.StringKind,
				"SELect", types.StringKind,
				"anD", types.StringKind),
			expectedRows: Rs(NewResultSetRow(types.String("1"), types.String("aaa"), types.String("create"), types.String("1.1"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)

			if tt.tableName != "" {
				dtestutils.CreateTestTable(t, dEnv, tt.tableName, tt.tableSchema, tt.initialRows...)
			}
			if tt.additionalSetup != nil {
				tt.additionalSetup(t, dEnv)
			}

			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Select)

			if tt.expectedRows != nil && tt.expectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}

func TestBuildSelectQueryPipeline(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedSchema  schema.Schema
		expectedNumRows int
	}{
		{
			name:            "Test select *",
			query:           "select * from people",
			expectedNumRows: len([]row.Row{Homer, Marge, Bart, Lisa, Moe, Barney}),
			expectedSchema:  CompressSchema(PeopleTestSchema),
		},
		{
			name:  "Test select columns",
			query: "select age, id from people",
			expectedNumRows: len([]row.Row{Homer, Marge, Bart, Lisa, Moe, Barney}),
			expectedSchema: CompressSchema(PeopleTestSchema, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		CreateTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot(context.Background())

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			p, statement, _ := BuildSelectQueryPipeline(context.Background(), root, s)
			var outputRows int
			p.SetOutput(pipeline.ProcFuncForSinkFunc(
				func(r row.Row, props pipeline.ReadableMap) error {
					outputRows++
					return nil
				}))
			p.SetBadRowCallback(func(*pipeline.TransformRowFailure) (quit bool) {
				return true
			})
			p.Start()
			p.Wait()

			assert.Equal(t, tt.expectedNumRows, outputRows)
			assert.Equal(t, tt.expectedSchema, statement.ResultSetSchema)
		})
	}
}
