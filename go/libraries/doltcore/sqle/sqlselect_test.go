package sqle

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/types"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

func TestExecuteSelect(t *testing.T) {
	for _, test := range BasicSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			if test.SkipOnSqlEngine {
				t.Skip("Skipping test broken on Sql Engine")
			}
			testSelectQuery(t, test)
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
		// TODO: fix me
		// {
		// 	name:  "ambiguous column in select",
		// 	query: `select id from people p, episodes e, appearances a where a.episode_id = e.id and a.character_id = p.id`,
		// 	expectedErr: "Ambiguous column: 'id'",
		// },
		// {
		// 	name:  "ambiguous column in where",
		// 	query: `select p.*, e.* from people p, episodes e, appearances a where a.episode_id = id and a.character_id = id`,
		// 	expectedErr: "Ambiguous column: 'id'",
		// },
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
			name:  "Natural join with join clause, select subset of columns, order by clause",
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
			name:  "Natural join with join clause, select subset of columns, order by clause on non-selected column",
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
			testSelectQuery(t, SelectTest{
				Name:            tt.name,
				Query:           tt.query,
				ExpectedSchema:  tt.expectedSchema,
				ExpectedRows:    tt.expectedRows,
				ExpectedErr:     tt.expectedErr,
				AdditionalSetup: nil,
			})
		})
	}
}

// Tests of case sensitivity handling
func TestCaseSensitivity(t *testing.T) {
	tests := []struct {
		Name            string
		tableName       string
		tableSchema     schema.Schema
		initialRows     []row.Row
		AdditionalSetup SetupFn
		query           string
		expectedRows    []row.Row
		expectedSchema  schema.Schema
		expectedErr     string
	}{
		{
			Name:           "table name has mixed case, select lower case",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select test from mixedcase",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			Name:           "table name has mixed case, select upper case",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select test from MIXEDCASE",
			expectedSchema: NewResultSetSchema("test", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		// TODO: fix me
		// {
		// 	name:           "qualified select *",
		// 	tableName:      "MiXeDcAsE",
		// 	tableSchema:    newSchema("test", types.StringKind),
		// 	initialRows:    Rs(newRow(types.String("1"))),
		// 	query:          "select mixedcAse.* from MIXEDCASE",
		// 	expectedSchema: NewResultSetSchema("test", types.StringKind),
		// 	expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		// },
		{
			Name:           "qualified select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcAse.TeSt from MIXEDCASE",
			expectedSchema: NewResultSetSchema("TeSt", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		// TODO: fix me
		// {
		// 	name:           "table alias select *",
		// 	tableName:      "MiXeDcAsE",
		// 	tableSchema:    newSchema("test", types.StringKind),
		// 	initialRows:    Rs(newRow(types.String("1"))),
		// 	query:          "select Mc.* from MIXEDCASE as mc",
		// 	expectedSchema: NewResultSetSchema("test", types.StringKind),
		// 	expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		// },
		{
			Name:           "table alias select column",
			tableName:      "MiXeDcAsE",
			tableSchema:    NewSchema("test", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mC.TeSt from MIXEDCASE as MC",
			expectedSchema: NewResultSetSchema("TeSt", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		// TODO: fix me
		// {
		// 	name:        "multiple tables with the same case-insensitive name, exact match",
		// 	tableName:   "tableName",
		// 	tableSchema: newSchema("test", types.StringKind),
		// 	additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
		// 		dtestutils.CreateTestTable(t, dEnv, "TABLENAME", newSchema("test", types.StringKind))
		// 		dtestutils.CreateTestTable(t, dEnv, "tablename", newSchema("test", types.StringKind))
		// 	},
		// 	initialRows:    Rs(newRow(types.String("1"))),
		// 	query:          "select test from tableName",
		// 	expectedSchema: NewResultSetSchema("test", types.StringKind),
		// 	expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		// },
		// {
		// 	name:        "multiple tables with the same case-insensitive name, no exact match",
		// 	tableName:   "tableName",
		// 	tableSchema: newSchema("test", types.StringKind),
		// 	additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
		// 		dtestutils.CreateTestTable(t, dEnv, "TABLENAME", newSchema("test", types.StringKind))
		// 	},
		// 	initialRows: Rs(newRow(types.String("1"))),
		// 	query:       "select test from tablename",
		// 	expectedErr: "Ambiguous table: 'tablename'",
		// },
		// {
		// 	name:        "alias with same name as table",
		// 	tableName:   "tableName",
		// 	tableSchema: newSchema("test", types.StringKind),
		// 	additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
		// 		dtestutils.CreateTestTable(t, dEnv, "other", newSchema("othercol", types.StringKind))
		// 	},
		// 	initialRows: Rs(newRow(types.String("1"))),
		// 	query:       "select other.test from tablename as other, other",
		// 	expectedErr: "Non-unique table name / alias: 'other'",
		// },
		// {
		// 	name:        "two table aliases with same name",
		// 	tableName:   "tableName",
		// 	tableSchema: newSchema("test", types.StringKind),
		// 	additionalSetup: func(t *testing.T, dEnv *env.DoltEnv) {
		// 		dtestutils.CreateTestTable(t, dEnv, "other", newSchema("othercol", types.StringKind))
		// 	},
		// 	initialRows: Rs(newRow(types.String("1"))),
		// 	query:       "select bad.test from tablename as bad, other as bad",
		// 	expectedErr: "Non-unique table name / alias: 'bad'",
		// },
		{
			Name:           "column name has mixed case, select lower case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			Name:           "column name has mixed case, select upper case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select MIXEDCASE from test",
			expectedSchema: NewResultSetSchema("MIXEDCASE", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			Name:           "select uses incorrect case",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			Name:           "select with multiple matching columns, exact match",
			tableName:      "test",
			tableSchema:    NewSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"), types.String("2"))),
			query:          "select mixedcase from test",
			expectedSchema: NewResultSetSchema("mixedcase", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("2"))),
		},
		// TODO: fix me
		// {
		// 	name:           "select with multiple matching columns, exact case #2",
		// 	tableName:      "test",
		// 	tableSchema:    newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
		// 	initialRows:    Rs(newRow(types.String("1"), types.String("2"))),
		// 	query:          "select MiXeDcAsE from test",
		// 	expectedSchema: NewResultSetSchema("MiXeDcAsE", types.StringKind),
		// 	expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		// },
		// {
		// 	name:        "select with multiple matching columns, no exact match",
		// 	tableName:   "test",
		// 	tableSchema: newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
		// 	initialRows: Rs(newRow(types.String("1"), types.String("2"))),
		// 	query:       "select MIXEDCASE from test",
		// 	expectedErr: "Ambiguous column: 'MIXEDCASE'",
		// },
		// {
		// 	name:        "select with multiple matching columns, no exact match, table alias",
		// 	tableName:   "test",
		// 	tableSchema: newSchema("MiXeDcAsE", types.StringKind, "mixedcase", types.StringKind),
		// 	initialRows: Rs(newRow(types.String("1"), types.String("2"))),
		// 	query:       "select t.MIXEDCASE from test t",
		// 	expectedErr: "Ambiguous column: 'MIXEDCASE'",
		// },
		// TODO: this could be handled better (not change the case of the result set schema), but the parser will silently
		//  lower-case any column name expression that is a reserved word. Changing that is harder.
		{
			Name:      "column is reserved word, select not backticked",
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
			Name:      "column is reserved word, qualified with table alias",
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
			Name:      "column is reserved word, select not backticked #2",
			tableName: "test",
			tableSchema: NewSchema(
				"YeAr", types.StringKind),
			initialRows:    Rs(NewRow(types.String("1"))),
			query:          "select Year from test",
			expectedSchema: NewResultSetSchema("year", types.StringKind),
			expectedRows:   Rs(NewResultSetRow(types.String("1"))),
		},
		{
			Name:      "column is reserved word, select backticked",
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
			Name:      "column is reserved word, select backticked #2",
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
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)

			if tt.AdditionalSetup != nil {
				tt.AdditionalSetup(t, dEnv)
			}
			if tt.tableName != "" {
				dtestutils.CreateTestTable(t, dEnv, tt.tableName, tt.tableSchema, tt.initialRows...)
			}

			root, _ := dEnv.WorkingRoot(context.Background())

			rows, sch, err := executeSelect(context.Background(), tt.expectedSchema, root, tt.query)
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

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testSelectQuery(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	actualRows, sch, err := executeSelect(context.Background(), test.ExpectedSchema, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedRows, actualRows)
	assert.Equal(t, test.ExpectedSchema, sch)
}

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(ctx context.Context, targetSch schema.Schema, root *doltdb.RootValue, query string) ([]row.Row, schema.Schema, error) {
	db := NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	sqlCtx := sql.NewContext(ctx)

	var err error
	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	if targetSch == nil {
		return nil, nil, nil
	}

	doltRows := make([]row.Row, 0)
	var r sql.Row
	for r, err = iter.Next(); err == nil; r, err = iter.Next() {
		doltRows = append(doltRows, SqlRowToDoltRow(r, targetSch))
	}

	return doltRows, targetSch, nil
}