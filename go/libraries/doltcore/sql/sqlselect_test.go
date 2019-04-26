package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
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
		filter         rowFilterFn
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
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    bool
	}{
		{
			name:           "Test select * ",
			query:          "select * from people",
			expectedRows:   rs(homer, marge, bart, lisa, moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where < int",
			query:          "select * from people where age < 40",
			expectedRows:   rs(marge, bart, lisa),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where > int reversed",
			query:          "select * from people where 40 > age",
			expectedRows:   rs(marge, bart, lisa),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where <= int",
			query:          "select * from people where age <= 40",
			expectedRows:   rs(homer, marge, bart, lisa, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where >= int reversed",
			query:          "select * from people where 40 >= age",
			expectedRows:   rs(homer, marge, bart, lisa, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where > int",
			query:          "select * from people where age > 40",
			expectedRows:   rs(moe),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where < int reversed",
			query:          "select * from people where 40 < age",
			expectedRows:   rs(moe),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where >= int",
			query:          "select * from people where age >= 40",
			expectedRows:   rs(homer, moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where <= int reversed",
			query:          "select * from people where 40 <= age",
			expectedRows:   rs(homer, moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where > string",
			query:          "select * from people where last > 'Simpson'",
			expectedRows:   rs(moe),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where < string",
			query:          "select * from people where last < 'Simpson'",
			expectedRows:   rs(barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where = string",
			query:          "select * from people where last = 'Simpson'",
			expectedRows:   rs(homer, marge, bart, lisa),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where > float",
			query:          "select * from people where rating > 8.0",
			expectedRows:   rs(homer, bart, lisa),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where < float",
			query:          "select * from people where rating < 8.0",
			expectedRows:   rs(moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where = float",
			query:          "select * from people where rating = 8.0",
			expectedRows:   rs(marge),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where < float reversed",
			query:          "select * from people where 8.0 < rating",
			expectedRows:   rs(homer, bart, lisa),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where > float reversed",
			query:          "select * from people where 8.0 > rating",
			expectedRows:   rs(moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where = float reversed",
			query:          "select * from people where 8.0 = rating",
			expectedRows:   rs(marge),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where bool = ",
			query:          "select * from people where is_married = true",
			expectedRows:   rs(homer, marge),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where bool = false ",
			query:          "select * from people where is_married = false",
			expectedRows:   rs(bart, lisa, moe, barney),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where bool <> ",
			query:          "select * from people where is_married <> false",
			expectedRows:   rs(homer, marge),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test select *, where bool",
			query:          "select * from people where is_married",
			expectedRows:   rs(homer, marge),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:  "Test select subset of cols",
			query: "select first, last from people where age >= 40",
			expectedRows: rs(homer, moe, barney),
			expectedSchema: subsetSchema(untypedPeopleSch, "first", "last"),
		},
		{
			name:  "Test column aliases",
			query: "select first as f, last as l from people where age >= 40",
			expectedRows: rs(homer, moe, barney),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l"),
		},
		{
			name:  "Test column aliases in where clause",
			query: `select first as f, last as l from people where f = "Homer"`,
			expectedRows: rs(homer),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l"),
		},
		{
			name:  "Test column aliases in where clause, >",
			query: `select first as f, last as l from people where l > "Simpson"`,
			expectedRows: rs(moe),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l"),
		},
		{
			name:  "Test column aliases in where clause, <",
			query: `select first as f, last as l from people where "Simpson" < l`,
			expectedRows: rs(moe),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l"),
		},
		{
			name:  "Test table aliases",
			query: "select p.first as f, people.last as l from people p where p.f = 'Homer'",
			expectedRows: rs(homer),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l"),
		},
		{
			name:  "Test table aliases with bad alias",
			query: "select m.first as f, p.last as l from people p where p.f = 'Homer'",
			expectedErr: true,
		},
		{
			name:  "Test column aliases, all columns",
			query: `select first as f, last as l, is_married as married, age as a,
				rating as r, uuid as u, num_episodes as n from people
				where age >= 40`,
			expectedRows: rs(homer, moe, barney),
			expectedSchema: newUntypedSchema(firstTag, "f", lastTag, "l", isMarriedTag, "married", ageTag, "a",
				ratingTag, "r", uuidTag, "u", numEpisodesTag, "n"),
		},
		// TODO: implement joins to make this work
		//{
		//	name:  "Test selecting from multiple tables",
		//	query: `select * from people, episodes`,
		//	expectedRows: rs(homer, moe, barney),
		//	expectedSchema: concatSchemas(untypedPeopleSch, untypedEpisodesSch),
		//},
		{
			name:           "Test select *, not equals",
			query:          "select * from people where age <> 40",
			expectedRows:   rs(marge, bart, lisa, moe),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:           "Test empty result set",
			query:          "select * from people where age > 80",
			expectedRows:   rs(),
			expectedSchema: untypedPeopleSch,
		},
		{
			name:  "Test empty result set with columns",
			query: "select id, age from people where age > 80",
			expectedRows: rs(),
			expectedSchema: subsetSchema(untypedPeopleSch, "id", "age"),
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
		{
			name: "type mismatch in where clause",
			query: `select * from people where id = "0"`,
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot(context.Background())

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedRows != nil && tt.expectedSchema == nil {
				assert.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
				t.FailNow()
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s, tt.query)
			untypedRows := convertRows(t, tt.expectedRows, peopleTestSchema, tt.expectedSchema)
			if err != nil {
				assert.True(t, tt.expectedErr, err.Error())
			} else {
				assert.False(t, tt.expectedErr, "unexpected error")
			}
			assert.Equal(t, untypedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}

// Creates a new untyped schema as specified by the given pairs of column tags and names. Tags are normal ints, not
// uint64
func newUntypedSchema(colTagsAndNames ...interface{}) schema.Schema {
	if len(colTagsAndNames) % 2 != 0 {
		panic("Non-even number of inputs passed to newUntypedSchema")
	}

	cols := make([]schema.Column, len(colTagsAndNames) / 2)
	for i := 0; i < len(colTagsAndNames); i += 2 {
		tag := uint64(colTagsAndNames[i].(int))
		name := colTagsAndNames[i+1].(string)
		cols[i/2] = schema.NewColumn(name, tag, types.StringKind, false)
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
			name:            "Test select * ",
			query:           "select * from people",
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema:  untypedPeopleSch,
		},
		{
			name:  "Test select columns ",
			query: "select age, id from people",
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema: subsetSchema(untypedPeopleSch, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot(context.Background())

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			p, sch, _ := BuildSelectQueryPipeline(context.Background(), root, s, tt.query)
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