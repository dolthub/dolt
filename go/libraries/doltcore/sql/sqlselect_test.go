package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
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
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 1",
			query:          "select * from people limit 1",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 0",
			query:          "select * from people limit 0,1",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 1",
			query:          "select * from people limit 1,1",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 5",
			query:          "select * from people limit 5,1",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 1 offset 6",
			query:          "select * from people limit 6,1",
			expectedRows:   sqltestutil.rs(),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 0",
			query:          "select * from people limit 0",
			expectedRows:   sqltestutil.rs(),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, limit 0 offset 0",
			query:          "select * from people limit 0,0",
			expectedRows:   sqltestutil.rs(),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
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
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < int",
			query:          "select * from people where age < 40",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 1",
			query:          "select * from people where age < 40 limit 1",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 2",
			query:          "select * from people where age < 40 limit 2",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < int, limit 100",
			query:          "select * from people where age < 40 limit 100",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by int",
			query:          "select * from people order by id",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by int desc",
			query:          "select * from people order by id desc",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.moe, sqltestutil.lisa, sqltestutil.bart, sqltestutil.marge, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by float",
			query:          "select * from people order by rating",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.moe, sqltestutil.marge, sqltestutil.homer, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by string",
			query:          "select * from people order by first",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.bart, sqltestutil.homer, sqltestutil.lisa, sqltestutil.marge, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by string,string",
			query:          "select * from people order by last desc, first asc",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.bart, sqltestutil.homer, sqltestutil.lisa, sqltestutil.marge, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by with limit",
			query:          "select * from people order by first limit 2",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.bart),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, order by string,string with limit",
			query:          "select * from people order by last desc, first asc limit 2",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.bart),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where > int reversed",
			query:          "select * from people where 40 > age",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where <= int",
			query:          "select * from people where age <= 40",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where >= int reversed",
			query:          "select * from people where 40 >= age",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where > int",
			query:          "select * from people where age > 40",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < int reversed",
			query:          "select * from people where 40 < age",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where >= int",
			query:          "select * from people where age >= 40",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where <= int reversed",
			query:          "select * from people where 40 <= age",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where > string",
			query:          "select * from people where last > 'Simpson'",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < string",
			query:          "select * from people where last < 'Simpson'",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where = string",
			query:          "select * from people where last = 'Simpson'",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where > float",
			query:          "select * from people where rating > 8.0",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < float",
			query:          "select * from people where rating < 8.0",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where = float",
			query:          "select * from people where rating = 8.0",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where < float reversed",
			query:          "select * from people where 8.0 < rating",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where > float reversed",
			query:          "select * from people where 8.0 > rating",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where = float reversed",
			query:          "select * from people where 8.0 = rating",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where bool = ",
			query:          "select * from people where is_married = true",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where bool = false ",
			query:          "select * from people where is_married = false",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where bool <> ",
			query:          "select * from people where is_married <> false",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, where bool",
			query:          "select * from people where is_married",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, and clause",
			query:          "select * from people where is_married and age > 38",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, or clause",
			query:          "select * from people where is_married or age < 20",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, in clause string",
			query:          "select * from people where first in ('Homer', 'Marge')",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, in clause integer",
			query:          "select * from people where age in (-10, 40)",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		// TODO: fix this
		// {
		// 	name:           "select *, in clause float",
		// 	query:          "select * from people where rating in (-10.0, 8.5)",
		// 	expectedRows:   compressRows(peopleTestSchema, homer),
		// 	expectedSchema: compressSchema(peopleTestSchema),
		// },
		{
			name:        "select *, in clause, mixed types",
			query:       "select * from people where first in ('Homer', 40)",
			expectedErr: "Type mismatch: mixed types in list literal '('Homer', 40)'",
		},
		// TODO: fix this
		// {
		// 	name:        "select *, in clause, mixed numeric types",
		// 	query:       "select * from people where age in (-10.0, 40)",
		// 	expectedErr: "Type mismatch: mixed types in list literal '(-10.0, 40)'",
		// },
		{
			name:           "select *, not in clause",
			query:          "select * from people where first not in ('Homer', 'Marge')",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, in clause single element",
			query:          "select * from people where first in ('Homer')",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:        "select *, in clause single type mismatch",
			query:       "select * from people where first in (1.0)",
			expectedErr: "Type mismatch:",
		},
		{
			name:           "select *, is null clause ",
			query:          "select * from people where uuid is null",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, is not null clause ",
			query:          "select * from people where uuid is not null",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, is true clause ",
			query:          "select * from people where is_married is true",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, is not true clause ",
			query:          "select * from people where is_married is not true",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, is false clause ",
			query:          "select * from people where is_married is false",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, is not false clause ",
			query:          "select * from people where is_married is not false",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.marge),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:        "select *, is true clause on non-bool column",
			query:       "select * from people where age is true",
			expectedErr: "Type mismatch:",
		},
		{
			name:           "binary expression in select",
			query:          "select age + 1 as a from people where is_married order by a",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.Int(39)), sqltestutil.newResultSetRow(types.Int(41))),
			expectedSchema: newResultSetSchema("a", types.IntKind),
		},
		{
			name:           "and expression in select",
			query:          "select is_married and age >= 40 from people where last = 'Simpson' order by id limit 2",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.Bool(true)), sqltestutil.newResultSetRow(types.Bool(false))),
			expectedSchema: newResultSetSchema("is_married and age >= 40", types.BoolKind),
		},
		{
			name:  "or expression in select",
			query: "select first, age <= 10 or age >= 40 as not_marge from people where last = 'Simpson' order by id desc",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.String("Lisa"), types.Bool(true)),
				sqltestutil.newResultSetRow(types.String("Bart"), types.Bool(true)),
				sqltestutil.newResultSetRow(types.String("Marge"), types.Bool(false)),
				sqltestutil.newResultSetRow(types.String("Homer"), types.Bool(true)),
			),
			expectedSchema: newResultSetSchema("first", types.StringKind, "not_marge", types.BoolKind),
		},
		{
			name:           "unary expression in select",
			query:          "select -age as age from people where is_married order by age",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.Int(-40)), sqltestutil.newResultSetRow(types.Int(-38))),
			expectedSchema: newResultSetSchema("age", types.IntKind),
		},
		{
			name:           "unary expression in select, alias named after column",
			query:          "select -age as age from people where is_married order by people.age",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.Int(-38)), sqltestutil.newResultSetRow(types.Int(-40))),
			expectedSchema: newResultSetSchema("age", types.IntKind),
		},
		{
			name:           "select *, -column",
			query:          "select * from people where -rating = -8.5",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:        "select *, -column, string type",
			query:       "select * from people where -first = 'Homer'",
			expectedErr: "Unsupported type for unary - operation: varchar",
		},
		{
			name:           "select *, binary + in where",
			query:          "select * from people where age + 1 = 41",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, binary - in where",
			query:          "select * from people where age - 1 = 39",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, binary / in where",
			query:          "select * from people where age / 2 = 20",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, binary * in where",
			query:          "select * from people where age * 2 = 80",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, binary % in where",
			query:          "select * from people where age % 4 = 0",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select *, complex binary expr in where",
			query:          "select * from people where age / 4 + 2 * 2 = 14",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
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
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.lisa, sqltestutil.bart, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "select subset of cols",
			query:          "select first, last from people where age >= 40",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first", "last"), sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema, "first", "last"),
		},
		{
			name:           "column aliases",
			query:          "select first as f, last as l from people where age >= 40",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first", "last"), sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:           "duplicate column aliases",
			query:          "select first as f, last as f from people where age >= 40",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first", "last"), sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: newResultSetSchema("f", types.StringKind, "f", types.StringKind),
		},
		{
			name:           "column selected more than once",
			query:          "select first, first from people where age >= 40 order by id",
			expectedRows:   sqltestutil.rs(
				sqltestutil.newResultSetRow(types.String("Homer"), types.String("Homer")),
				sqltestutil.newResultSetRow(types.String("Moe"), types.String("Moe")),
				sqltestutil.newResultSetRow(types.String("Barney"), types.String("Barney")),
			),
			expectedSchema: newResultSetSchema("first", types.StringKind, "first", types.StringKind),
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
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first"), sqltestutil.lisa, sqltestutil.bart, sqltestutil.marge, sqltestutil.barney, sqltestutil.homer, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema, "first"),
		},
		{
			name:           "column aliases with order by",
			query:          "select first as f from people order by age, f",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first"), sqltestutil.lisa, sqltestutil.bart, sqltestutil.marge, sqltestutil.barney, sqltestutil.homer, sqltestutil.moe),
			expectedSchema: newResultSetSchema("f", types.StringKind),
		},
		{
			name:        "ambiguous column in order by",
			query:       "select first as f, last as f from people order by f",
			expectedErr: "Ambiguous column: 'f'",
		},
		{
			name:           "table aliases",
			query:          "select p.first as f, people.last as l from people p where p.first = 'Homer'",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first", "last"), sqltestutil.homer),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:           "table aliases without column aliases",
			query:          "select p.first, people.last from people p where p.first = 'Homer'",
			expectedRows:   compressRows(resultset.SubsetSchema(sqltestutil.peopleTestSchema, "first", "last"), sqltestutil.homer),
			expectedSchema: newResultSetSchema("first", types.StringKind, "last", types.StringKind),
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
			expectedRows: compressRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.moe, sqltestutil.barney),
			expectedSchema: newResultSetSchema("i", types.IntKind, "f", types.StringKind,
				"l", types.StringKind, "m", types.BoolKind, "a", types.IntKind, "r", types.FloatKind,
				"u", types.UUIDKind, "n", types.UintKind),
		},
		{
			name:           "select *, not equals",
			query:          "select * from people where age <> 40",
			expectedRows:   compressRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "empty result set",
			query:          "select * from people where age > 80",
			expectedRows:   sqltestutil.rs(),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:           "empty result set with columns",
			query:          "select id, age from people where age > 80",
			expectedRows:   sqltestutil.rs(),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema, "id", "age"),
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
			name:         "unsupported comparison",
			query:        "select * from people where function(first)",
			expectedErr:  "not supported",
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
			sqltestutil.createTestDatabase(dEnv, t)
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
			expectedRows: sqltestutil.rs(
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep4),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep4),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep4),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep4),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep4),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep4),
			),
			expectedSchema: compressSchemas(sqltestutil.peopleTestSchema, sqltestutil.episodesTestSchema),
		},
		{
			name:  "Natural join with where clause",
			query: `select * from people p, episodes e where e.id = p.id`,
			expectedRows: sqltestutil.rs(
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep4),
			),
			expectedSchema: compressSchemas(sqltestutil.peopleTestSchema, sqltestutil.episodesTestSchema),
		},
		{
			name:  "Three table natural join with where clause",
			query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
			expectedRows: sqltestutil.rs(
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep3),
			),
			expectedSchema: compressSchemas(sqltestutil.peopleTestSchema, sqltestutil.episodesTestSchema),
		},
		{
			name:  "ambiguous column in select",
			query: `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
			expectedErr: "Ambiguous column: 'id'",
		},
		{
			name:  "ambiguous column in where",
			query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
			expectedErr: "Ambiguous column: 'id'",
		},
		{
			name:  "Natural join with where clause, select subset of columns",
			query: `select e.id, p.id, e.name, p.first, p.last from people p, episodes e where e.id = p.id`,
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with where clause and column aliases",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with where clause and quoted column alias",
			query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p, episodes e where e.id = p.id",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause",
			query: `select * from people p join episodes e on e.id = p.id`,
			expectedRows: sqltestutil.rs(
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep4),
			),
			expectedSchema: compressSchemas(sqltestutil.peopleTestSchema, sqltestutil.episodesTestSchema),
		},
		{
			name:  "Three table natural join with join clause",
			query: `select p.*, e.* from people p join appearances a on a.character_id = p.id join episodes e on a.episode_id = e.id`,
			expectedRows: sqltestutil.rs(
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.homer, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep1),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.marge, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.bart, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.lisa, sqltestutil.episodesTestSchema, sqltestutil.ep3),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.moe, sqltestutil.episodesTestSchema, sqltestutil.ep2),
				concatRows(sqltestutil.peopleTestSchema, sqltestutil.barney, sqltestutil.episodesTestSchema, sqltestutil.ep3),
			),
			expectedSchema: compressSchemas(sqltestutil.peopleTestSchema, sqltestutil.episodesTestSchema),
		},
		{
			name:  "Natural join with join clause, select subset of columns",
			query: `select e.id, p.id, e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause, select subset of columns, join columns not selected",
			query: `select e.name, p.first, p.last from people p join episodes e on e.id = p.id`,
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause, select subset of columns, order by clause",
			query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by e.name`,
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause, select subset of columns, order by clause on non-selected column",
			query: `select e.id, p.id, e.name, p.first, p.last from people p 
							join episodes e on e.id = p.id
							order by age`,
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and column aliases",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and column aliases, order by",
			query: "select e.id as eid, p.id as pid, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id order by ename",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("eid", types.IntKind, "pid", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
		{
			name:  "Natural join with join clause and quoted column alias",
			query: "select e.id as eid, p.id as `p.id`, e.name as ename, p.first as pfirst, p.last last from people p join episodes e on e.id = p.id",
			expectedRows: sqltestutil.rs(
				sqltestutil.newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				sqltestutil.newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("eid", types.IntKind, "p.id", types.IntKind,
				"ename", types.StringKind, "pfirst", types.StringKind, "last", types.StringKind),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			sqltestutil.createTestDatabase(dEnv, t)
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
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select test from mixedcase",
			expectedSchema: newResultSetSchema("test", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "table name has mixed case, select upper case",
			tableName:      "MiXeDcAsE",
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select test from MIXEDCASE",
			expectedSchema: newResultSetSchema("test", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "qualified select *",
			tableName:      "MiXeDcAsE",
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select mixedcAse.* from MIXEDCASE",
			expectedSchema: newResultSetSchema("test", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "qualified select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select mixedcAse.TeSt from MIXEDCASE",
			expectedSchema: newResultSetSchema("TeSt", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "table alias select *",
			tableName:      "MiXeDcAsE",
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select Mc.* from MIXEDCASE as mc",
			expectedSchema: newResultSetSchema("test", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "table alias select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    newSchema("test", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select mC.TeSt from MIXEDCASE as MC",
			expectedSchema: newResultSetSchema("TeSt", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:        "multiple tables with the same case-insensitive name, exact match",
			tableName:   "tableName",
			tableSchema: newSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "TABLENAME", newSchema("test", types.StringKind))
				dtestutils.CreateTestTable(t, dEnv, "tablename", newSchema("test", types.StringKind))
			},
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select test from tableName",
			expectedSchema: newResultSetSchema("test", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:        "multiple tables with the same case-insensitive name, no exact match",
			tableName:   "tableName",
			tableSchema: newSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "TABLENAME", newSchema("test", types.StringKind))
			},
			initialRows: sqltestutil.rs(newRow(types.String("1"))),
			query:       "select test from tablename",
			expectedErr: "Ambiguous table: 'tablename'",
		},
		{
			name:        "alias with same name as table",
			tableName:   "tableName",
			tableSchema: newSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "other", newSchema("othercol", types.StringKind))
			},
			initialRows: sqltestutil.rs(newRow(types.String("1"))),
			query:       "select other.test from tablename as other, other",
			expectedErr: "Non-unique table name / alias: 'other'",
		},
		{
			name:        "two table aliases with same name",
			tableName:   "tableName",
			tableSchema: newSchema("test", types.StringKind),
			additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
				dtestutils.CreateTestTable(t, dEnv, "other", newSchema("othercol", types.StringKind))
			},
			initialRows: sqltestutil.rs(newRow(types.String("1"))),
			query:       "select bad.test from tablename as bad, other as bad",
			expectedErr: "Non-unique table name / alias: 'bad'",
		},
		{
			name:           "column name has mixed case, select lower case",
			tableName:      "test",
			tableSchema:    newSchema("MiXeDcAsE", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: newResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "column name has mixed case, select upper case",
			tableName:      "test",
			tableSchema:    newSchema("MiXeDcAsE", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select MIXEDCASE from test",
			expectedSchema: newResultSetSchema("MIXEDCASE", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "select uses incorrect case",
			tableName:      "test",
			tableSchema:    newSchema("MiXeDcAsE", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: newResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:           "select with multiple matching columns, exact match",
			tableName:      "test",
			tableSchema:    newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"), types.String("2"))),
			query:          "select mixedcase from test",
			expectedSchema: newResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("2"))),
		},
		{
			name:           "select with multiple matching columns, exact case #2",
			tableName:      "test",
			tableSchema:    newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"), types.String("2"))),
			query:          "select MiXeDcAsE from test",
			expectedSchema: newResultSetSchema("MiXeDcAsE", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:        "select with multiple matching columns, no exact match",
			tableName:   "test",
			tableSchema: newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows: sqltestutil.rs(newRow(types.String("1"), types.String("2"))),
			query:       "select MIXEDCASE from test",
			expectedErr: "Ambiguous column: 'MIXEDCASE'",
		},
		{
			name:        "select with multiple matching columns, no exact match, table alias",
			tableName:   "test",
			tableSchema: newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows: sqltestutil.rs(newRow(types.String("1"), types.String("2"))),
			query:       "select t.MIXEDCASE from test t",
			expectedErr: "Ambiguous column: 'MIXEDCASE'",
		},
		// TODO: this could be handled better (not change the case of the result set schema), but the parser will silently
		//  lower-case any column name expression that is a reserved word. Changing that is harder.
		{
			name:        "column is reserved word, select not backticked",
			tableName:   "test",
			tableSchema: newSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: sqltestutil.rs(
				newRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select Timestamp from test",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
			expectedSchema: newResultSetSchema("timestamp", types.StringKind),
		},
		{
			name:        "column is reserved word, qualified with table alias",
			tableName:   "test",
			tableSchema: newSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: sqltestutil.rs(
				newRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select t.Timestamp from test as t",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
			expectedSchema: newResultSetSchema("timestamp", types.StringKind),
		},
		{
			name:        "column is reserved word, select not backticked #2",
			tableName:   "test",
			tableSchema: newSchema(
				"YeAr", types.StringKind),
			initialRows:    sqltestutil.rs(newRow(types.String("1"))),
			query:          "select Year from test",
			expectedSchema: newResultSetSchema("year", types.StringKind),
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
		},
		{
			name:        "column is reserved word, select backticked",
			tableName:   "test",
			tableSchema: newSchema(
				"Timestamp", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: sqltestutil.rs(
				newRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create")),
			),
			query:          "select `Timestamp` from test",
			expectedRows:   sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"))),
			expectedSchema: newResultSetSchema("Timestamp", types.StringKind),
		},
		{
			name:        "column is reserved word, select backticked #2",
			tableName:   "test",
			tableSchema: newSchema(
				"Year", types.StringKind,
				"and", types.StringKind,
				"or", types.StringKind,
				"select", types.StringKind),
			initialRows: sqltestutil.rs(newRow(types.String("1"), types.String("1.1"), types.String("aaa"), types.String("create"))),
			query:       "select `Year`, `OR`, `SELect`, `anD` from test",
			expectedSchema: newResultSetSchema(
				"Year", types.StringKind,
				"OR", types.StringKind,
				"SELect", types.StringKind,
				"anD", types.StringKind),
			expectedRows: sqltestutil.rs(sqltestutil.newResultSetRow(types.String("1"), types.String("aaa"), types.String("create"), types.String("1.1"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			sqltestutil.createTestDatabase(dEnv, t)

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

// Creates a new row with the values given, using ascending tag numbers starting at 0.
// Uses the first value as the primary key.
func newRow(colVals ...types.Value) row.Row {
	var cols []schema.Column
	taggedVals := make(row.TaggedValues)
	var tag int64
	for _, val := range colVals {
		isPk := tag == 0
		var constraints []schema.ColConstraint
		if isPk {
			constraints = append(constraints, schema.NotNullConstraint{})
		}
		cols = append(cols, schema.NewColumn(strconv.FormatInt(tag, 10), uint64(tag), val.Kind(), isPk, constraints...))

		taggedVals[uint64(tag)] = val
		tag++
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err.Error())
	}

	sch := schema.SchemaFromCols(colColl)

	return row.New(sch, taggedVals)
}

// Creates a new schema with the pairs of column names and types given, using ascending tag numbers starting at 0.
// Uses the first column as the primary key.
func newSchema(colNamesAndTypes ...interface{}) schema.Schema {
	if len(colNamesAndTypes) % 2 != 0 {
		panic("Non-even number of inputs passed to newResultSetSchema")
	}

	cols := make([]schema.Column, len(colNamesAndTypes) / 2)
	for i := 0; i < len(colNamesAndTypes); i += 2 {
		name := colNamesAndTypes[i].(string)
		nomsKind := colNamesAndTypes[i+1].(types.NomsKind)

		isPk := i/2 == 0
		var constraints []schema.ColConstraint
		if isPk {
			constraints = append(constraints, schema.NotNullConstraint{})
		}
		cols[i/2] = schema.NewColumn(name, uint64(i/2), nomsKind, isPk, constraints...)
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err.Error())
	}

	return schema.SchemaFromCols(colColl)
}

// Returns the logical concatenation of the schemas and rows given, rewriting all tag numbers to begin at zero. The row
// returned will have a new schema identical to the result of compressSchema.
func concatRows(schemasAndRows ...interface{}) row.Row {
	if len(schemasAndRows) % 2 != 0 {
		panic("Non-even number of inputs passed to concatRows")
	}

	taggedVals := make(row.TaggedValues)
	cols := make([]schema.Column, 0)
	var itag uint64
	for i := 0; i < len(schemasAndRows); i += 2 {
		sch := schemasAndRows[i].(schema.Schema)
		r := schemasAndRows[i+1].(row.Row)
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			val, ok := r.GetColVal(tag)
			if ok {
				taggedVals[itag] = val
			}

			col.Tag = itag
			cols = append(cols, col)
			itag++

			return false
		})
	}

	colCol, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err.Error())
	}

	return row.New(schema.UnkeyedSchemaFromCols(colCol), taggedVals)
}

// Rewrites the tag numbers for the row given to begin at zero and be contiguous, just like result set schemas. We don't
// want to just use the field mappings in the result set schema used by sqlselect, since that would only demonstrate
// that the code was consistent with itself, not actually correct.
func compressRow(sch schema.Schema, r row.Row) row.Row {
	var itag uint64
	compressedRow := make(row.TaggedValues)

	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if val, ok := r.GetColVal(tag); ok {
			compressedRow[itag] = val
		}
		itag++
		return false
	})

	// call to compress schema is a no-op in most cases
	return row.New(compressSchema(sch), compressedRow)
}

// Compresses each of the rows given ala compressRow
func compressRows(sch schema.Schema, rs ...row.Row, ) []row.Row {
	compressed := make([]row.Row, len(rs))
	for i := range rs {
		compressed[i] = compressRow(sch, rs[i])
	}
	return compressed
}

// Rewrites the tag numbers for the schema given to start at 0, just like result set schemas. If one or more column
// names are given, only those column names are included in the compressed schema. The column list can also be used to
// reorder the columns as necessary.
func compressSchema(sch schema.Schema, colNames ...string) schema.Schema {
	var itag uint64
	var cols []schema.Column

	if len(colNames) > 0 {
		cols = make([]schema.Column, len(colNames))
		for _, colName := range colNames {
			column, ok := sch.GetAllCols().GetByName(colName)
			if !ok {
				panic("No column found for column name " + colName)
			}
			column.Tag = itag
			cols[itag] = column
			itag++
		}
	} else {
		cols = make([]schema.Column, sch.GetAllCols().Size())
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			col.Tag = itag
			cols[itag] = col
			itag++
			return false
		})
	}

	colCol, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err.Error())
	}

	return schema.UnkeyedSchemaFromCols(colCol)
}

// Rewrites the tag numbers for the schemas given to start at 0, just like result set schemas.
func compressSchemas(schs ...schema.Schema) schema.Schema {
	var itag uint64
	var cols []schema.Column

	cols = make([]schema.Column, 0)
	for _, sch := range schs {
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			col.Tag = itag
			cols = append(cols, col)
			itag++
			return false
		})
	}

	colCol, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err.Error())
	}

	return schema.UnkeyedSchemaFromCols(colCol)
}

// Creates a new schema for a result set specified by the given pairs of column names and types. Column names are
// strings, types are NomsKinds.
func newResultSetSchema(colNamesAndTypes ...interface{}) schema.Schema {

	if len(colNamesAndTypes) % 2 != 0 {
		panic("Non-even number of inputs passed to newResultSetSchema")
	}

	cols := make([]schema.Column, len(colNamesAndTypes) / 2)
	for i := 0; i < len(colNamesAndTypes); i += 2 {
		name := colNamesAndTypes[i].(string)
		nomsKind := colNamesAndTypes[i+1].(types.NomsKind)
		cols[i/2] = schema.NewColumn(name, uint64(i/2), nomsKind, false)
	}

	collection, err := schema.NewColCollection(cols...)
	if err != nil {
		panic("unexpected error " + err.Error())
	}
	return schema.UnkeyedSchemaFromCols(collection)
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
			expectedNumRows: len([]row.Row{sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney}),
			expectedSchema:  compressSchema(sqltestutil.peopleTestSchema),
		},
		{
			name:  "Test select columns",
			query: "select age, id from people",
			expectedNumRows: len([]row.Row{sqltestutil.homer, sqltestutil.marge, sqltestutil.bart, sqltestutil.lisa, sqltestutil.moe, sqltestutil.barney}),
			expectedSchema: compressSchema(sqltestutil.peopleTestSchema, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		sqltestutil.createTestDatabase(dEnv, t)
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