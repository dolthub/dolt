package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/xwb1989/sqlparser"
)

const (
	idTag        = 0
	firstTag     = 1
	lastTag      = 2
	isMarriedTag = 3
	ageTag       = 4
	emptyTag     = 5
)

var testSch = createTestSchema()
var untypedSch = untyped.UntypeSchema(testSch)

var tableName = "people"

func createTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.UintKind, false),
		schema.NewColumn("empty", emptyTag, types.IntKind, false),
	)
	return schema.SchemaFromCols(colColl)
}

func newRow(id int, first, last string, isMarried bool, age int) row.Row {
	vals := row.TaggedValues{
		idTag: types.Int(id),
		firstTag: types.String(first),
		lastTag: types.String(last),
		isMarriedTag: types.Bool(isMarried),
		ageTag: types.Uint(age),
	}

	return row.New(testSch, vals)
}

var homer = newRow(0, "Homer", "Simpson", true, 40)
var marge = newRow(1, "Marge", "Simpson", true, 38)
var bart = newRow(2, "Bart", "Simpson", false, 10)
var lisa = newRow(3, "Lisa", "Simpson", false, 8)
var moe = newRow(4, "Moe", "Szyslak", false, 48)
var barney = newRow(5, "Barney", "Gumble", false, 40)

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
			name:  "Test unsupported comparison",
			query: "select * from people where first in ('homer')",
			expectedRows: nil, // not the same as empty result set
			expectedErr: true,
		},
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


func rs(rows... row.Row) []row.Row {
	return rows
}

// Returns a subset of the schema given
func subsetSchema(sch schema.Schema, colNames ...string) schema.Schema {
	colColl := sch.GetAllCols()

	if len(colNames) > 0 {
		cols := make([]schema.Column, 0, len(colNames))
		for _, name := range colNames {
			if col, ok := colColl.GetByName(name); !ok {
				panic("Unrecognized name")
			} else {
				cols = append(cols, col)
			}
		}
		colColl, _ = schema.NewColCollection(cols...)
	}

	return schema.SchemaFromCols(colColl)
}

// Converts the rows given, having the schema given, to an untyped (string-typed) row. Only the column names specified
// will be included.
func untypeRows(t *testing.T, rows []row.Row, colNames []string, tableSch schema.Schema) []row.Row {
	// Zero typing make the nil slice and the empty slice appear equal to most functions, but they are semantically
	// distinct.
	if rows == nil {
		return nil
	}

	untyped := make([]row.Row, 0, len(rows))
	for _, r := range rows {
		untyped = append(untyped, untypeRow(t, r, colNames, tableSch))
	}
	return untyped
}

// Converts the row given, having the schema given, to an untyped (string-typed) row. Only the column names specified
// will be included.
func untypeRow(t *testing.T, r row.Row, colNames []string, tableSch schema.Schema) row.Row {
	outSch := subsetSchema(tableSch, colNames...)

	mapping, err := rowconv.TagMapping(tableSch, untyped.UntypeSchema(outSch))
	assert.Nil(t, err, "failed to create untyped mapping")

	rConv, _ := rowconv.NewRowConverter(mapping)
	untyped, err := rConv.Convert(r)
	assert.Nil(t, err, "failed to untyped row to untyped")
	return untyped
}

func createTestDatabase(dEnv *env.DoltEnv, t *testing.T) {
	imt := table.NewInMemTable(testSch)

	for _, r := range []row.Row{homer, marge, bart, lisa, moe, barney} {
		imt.AppendRow(r)
	}

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(dEnv.DoltDB.ValueReadWriter(), testSch)

	_, _, err := table.PipeRows(rd, wr, false)
	rd.Close()
	wr.Close()

	assert.Nil(t, err, "Failed to seed initial data")

	err = dEnv.PutTableToWorking(*wr.GetMap(), wr.GetSchema(), tableName)
	assert.Nil(t, err,"Unable to put initial value of table in in mem noms db")
}
