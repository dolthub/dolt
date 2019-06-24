package sql

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
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
	for _, tt := range BasicSelectTests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, err := sqlparser.Parse(tt.Query)
			if err != nil {
				assert.FailNow(t, "Couldn't parse query "+tt.Query, "%v", err.Error())
			}

			s := sqlStatement.(*sqlparser.Select)

			if tt.ExpectedRows != nil && tt.ExpectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s)

			if len(tt.ExpectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.ExpectedErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.ExpectedRows, rows)
			assert.Equal(t, tt.ExpectedSchema, sch)
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
	for _, tt := range CaseSensitivityTests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)

			if tt.AdditionalSetup != nil {
				tt.AdditionalSetup(t, dEnv)
			}

			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, _ := sqlparser.Parse(tt.Query)
			s := sqlStatement.(*sqlparser.Select)

			if tt.ExpectedRows != nil && tt.ExpectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s)
			if len(tt.ExpectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.ExpectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.ExpectedRows, rows)
			assert.Equal(t, tt.ExpectedSchema, sch)
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
