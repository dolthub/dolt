package sql

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/xwb1989/sqlparser"
)


// Tests that the basic SelectAndLimit
func Test_selectTransform_limitAndFilter(t *testing.T) {
	var noMoreCallbackCalled = false
	var noMoreCallback = func() {
		noMoreCallbackCalled = true
	}

	type fields struct {
		noMoreCallback func()
		filter         filterFn
		limit          int
		count          int
	}
	type args struct {
		inRow row.Row
		props pipeline.ReadableMap
	}
	tests := []struct {
		name                  string
		fields                fields
		args                  args
		expectedRow           []*pipeline.TransformedRowResult
		expectedBadRowDetails string
		noMoreCalled          bool
	}{
		{
			name: "true no limit",
			fields: fields{
				noMoreCallback: noMoreCallback,
				filter: func(r row.Row) (matchesFilter bool) { return true },
				limit:  -1,
			},
			args: args{ homer, pipeline.NoProps },
			expectedRow: transformedRowResults(homer),
			expectedBadRowDetails: "",
		},
		{
			name: "false no limit",
			fields: fields{
				noMoreCallback: noMoreCallback,
				filter: func(r row.Row) (matchesFilter bool) { return false },
				limit:  -1,
			},
			args: args{ homer, pipeline.NoProps },
			expectedRow: transformedRowResults(),
			expectedBadRowDetails: "",
		},
		{
			name: "true limit 1",
			fields: fields{
				noMoreCallback: noMoreCallback,
				filter: func(r row.Row) (matchesFilter bool) { return true },
				limit:  1,
				count: 1,
			},
			args: args{ homer, pipeline.NoProps },
			expectedRow: transformedRowResults(),
			expectedBadRowDetails: "",
			noMoreCalled: true,
		},
		{
			name: "false limit 1",
			fields: fields{
				noMoreCallback: noMoreCallback,
				filter: func(r row.Row) (matchesFilter bool) { return false },
				limit:  1,
				count: 1,
			},
			args: args{ homer, pipeline.NoProps },
			expectedRow: transformedRowResults(),
			expectedBadRowDetails: "",
			noMoreCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noMoreCallbackCalled = false
			st := &selectTransform{
				noMoreCallback: tt.fields.noMoreCallback,
				filter:         tt.fields.filter,
				limit:          tt.fields.limit,
				count:          tt.fields.count,
			}
			row, badRowDetails := st.limitAndFilter(tt.args.inRow, tt.args.props)
			assert.Equal(t, tt.expectedRow, row)
			assert.Equal(t, tt.expectedBadRowDetails, badRowDetails)
			assert.Equal(t, tt.noMoreCalled, noMoreCallbackCalled)
		})
	}
}

func transformedRowResults(rows... row.Row) []*pipeline.TransformedRowResult {
	var r []*pipeline.TransformedRowResult
	for _, v := range rows {
		r = append(r, transformedRowWithoutProps(v))
	}
	return r
}

func transformedRowWithoutProps(r row.Row) *pipeline.TransformedRowResult {
	return &pipeline.TransformedRowResult{r, nil}
}

func TestExecuteSelect(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		columns        []string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    bool
	}{
		{
			name:  "Test select * ",
			query: "select * from people",
			expectedRows: rs(homer, marge, bart, lisa, moe, barney),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select *, where < int",
			query: "select * from people where age < 40",
			expectedRows: rs(marge, bart, lisa),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select *, where <= int",
			query: "select * from people where age <= 40",
			expectedRows: rs(homer, marge, bart, lisa, barney),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select *, where > int",
			query: "select * from people where age > 40",
			expectedRows: rs(moe),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select *, where >= int",
			query: "select * from people where age >= 40",
			expectedRows: rs(homer, moe, barney),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select *, where > float",
			query: "select * from people where rating > 8.0",
			expectedRows: rs(homer, bart, lisa),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select subset of cols",
			query: "select first,last from people where age >= 40",
			columns: []string { "first", "last" },
			expectedRows: rs(homer, moe, barney),
			expectedSchema: subsetSchema(untypedSch, "first", "last"),
		},
		{
			name:  "Test select *, not equals",
			query: "select * from people where age <> 40",
			expectedRows: rs(marge, bart, lisa, moe),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test empty result set",
			query: "select * from people where age > 80",
			expectedRows: rs(),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test empty result set with columns",
			query: "select id, age from people where age > 80",
			expectedRows: rs(),
			expectedSchema: subsetSchema(untypedSch, "id", "age"),
		},
		{
			name:  "Test select * unknown column",
			query: "select * from people where dne > 8.0",
			expectedErr: true,
		},
		{
			name:  "Test unsupported comparison",
			query: "select * from people where first in ('Homer')",
			expectedRows: nil, // not the same as empty result set
			expectedErr: true,
		},
		// This should fail but doesn't.
		//{
		//	name: "type mismatch in where clause",
		//	query: `select * from people where id = "0"`,
		//	expectedErr: true,
		//},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot()

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			rows, sch, err := ExecuteSelect(root, s, tt.query)
			untypedRows := untypeRows(t, tt.expectedRows, tt.columns, testSch)
			assert.Equal(t, tt.expectedErr, err != nil)
			assert.Equal(t, untypedRows, rows)
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
			name:  "Test select * ",
			query: "select * from people",
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema: untypedSch,
		},
		{
			name:  "Test select columns ",
			query: "select age, id from people",
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema: subsetSchema(untypedSch, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot()

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			p, sch, _ := BuildSelectQueryPipeline(root, s, tt.query)
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
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}