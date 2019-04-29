package sql

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
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
			expectedRows:   compressRows(peopleTestSchema, homer, marge, bart, lisa, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where < int",
			query:          "select * from people where age < 40",
			expectedRows:   compressRows(peopleTestSchema, marge, bart, lisa),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where > int reversed",
			query:          "select * from people where 40 > age",
			expectedRows:   compressRows(peopleTestSchema, marge, bart, lisa),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where <= int",
			query:          "select * from people where age <= 40",
			expectedRows:   compressRows(peopleTestSchema, homer, marge, bart, lisa, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where >= int reversed",
			query:          "select * from people where 40 >= age",
			expectedRows:   compressRows(peopleTestSchema, homer, marge, bart, lisa, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where > int",
			query:          "select * from people where age > 40",
			expectedRows:   compressRows(peopleTestSchema, moe),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where < int reversed",
			query:          "select * from people where 40 < age",
			expectedRows:   compressRows(peopleTestSchema, moe),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where >= int",
			query:          "select * from people where age >= 40",
			expectedRows:   compressRows(peopleTestSchema, homer, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where <= int reversed",
			query:          "select * from people where 40 <= age",
			expectedRows:   compressRows(peopleTestSchema, homer, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where > string",
			query:          "select * from people where last > 'Simpson'",
			expectedRows:   compressRows(peopleTestSchema, moe),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where < string",
			query:          "select * from people where last < 'Simpson'",
			expectedRows:   compressRows(peopleTestSchema, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where = string",
			query:          "select * from people where last = 'Simpson'",
			expectedRows:   compressRows(peopleTestSchema, homer, marge, bart, lisa),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where > float",
			query:          "select * from people where rating > 8.0",
			expectedRows:   compressRows(peopleTestSchema, homer, bart, lisa),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where < float",
			query:          "select * from people where rating < 8.0",
			expectedRows:   compressRows(peopleTestSchema, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where = float",
			query:          "select * from people where rating = 8.0",
			expectedRows:   compressRows(peopleTestSchema, marge),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where < float reversed",
			query:          "select * from people where 8.0 < rating",
			expectedRows:   compressRows(peopleTestSchema, homer, bart, lisa),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where > float reversed",
			query:          "select * from people where 8.0 > rating",
			expectedRows:   compressRows(peopleTestSchema, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where = float reversed",
			query:          "select * from people where 8.0 = rating",
			expectedRows:   compressRows(peopleTestSchema, marge),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where bool = ",
			query:          "select * from people where is_married = true",
			expectedRows:   compressRows(peopleTestSchema, homer, marge),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where bool = false ",
			query:          "select * from people where is_married = false",
			expectedRows:   compressRows(peopleTestSchema, bart, lisa, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where bool <> ",
			query:          "select * from people where is_married <> false",
			expectedRows:   compressRows(peopleTestSchema, homer, marge),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test select *, where bool",
			query:          "select * from people where is_married",
			expectedRows:   compressRows(peopleTestSchema, homer, marge),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:  "Test select subset of cols",
			query: "select first, last from people where age >= 40",
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), homer, moe, barney),
			expectedSchema: compressSchema(peopleTestSchema,"first", "last"),
		},
		{
			name:  "Test column aliases",
			query: "select first as f, last as l from people where age >= 40",
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), homer, moe, barney),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:  "Test column aliases in where clause",
			query: `select first as f, last as l from people where f = "Homer"`,
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), homer),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:  "Test column aliases in where clause, >",
			query: `select first as f, last as l from people where l > "Simpson"`,
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), moe),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:  "Test column aliases in where clause, <",
			query: `select first as f, last as l from people where "Simpson" < l`,
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), moe),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:  "Test table aliases",
			query: "select p.first as f, people.last as l from people p where p.f = 'Homer'",
			expectedRows: compressRows(resultset.SubsetSchema(peopleTestSchema,"first", "last"), homer),
			expectedSchema: newResultSetSchema("f", types.StringKind, "l", types.StringKind),
		},
		{
			name:  "Test table aliases with bad alias",
			query: "select m.first as f, p.last as l from people p where p.f = 'Homer'",
			expectedErr: true,
		},
		{
			name:  "Test column aliases, all columns",
			query: `select id as i, first as f, last as l, is_married as m, age as a,
				rating as r, uuid as u, num_episodes as n from people
				where age >= 40`,
			expectedRows: compressRows(peopleTestSchema, homer, moe, barney),
			expectedSchema: newResultSetSchema("i", types.IntKind, "f", types.StringKind,
				"l", types.StringKind, "m", types.BoolKind, "a", types.IntKind, "r", types.FloatKind,
				"u", types.UUIDKind, "n", types.UintKind),
		},
		{
			name:           "Test select *, not equals",
			query:          "select * from people where age <> 40",
			expectedRows:   compressRows(peopleTestSchema, marge, bart, lisa, moe),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:           "Test empty result set",
			query:          "select * from people where age > 80",
			expectedRows:   rs(),
			expectedSchema: compressSchema(peopleTestSchema),
		},
		{
			name:  "Test empty result set with columns",
			query: "select id, age from people where age > 80",
			expectedRows: rs(),
			expectedSchema: compressSchema(peopleTestSchema, "id", "age"),
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
		root, _ := dEnv.WorkingRoot()

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedRows != nil && tt.expectedSchema == nil {
				assert.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
				t.FailNow()
			}

			rows, sch, err := ExecuteSelect(root, s)
			if err != nil {
				assert.True(t, tt.expectedErr, err.Error())
			} else {
				assert.False(t, tt.expectedErr, "unexpected error")
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
		expectedErr    bool
	}{
		{
			name:  "Test full cross product",
			query: `select * from people, episodes`,
			expectedRows: rs(
				concatRows(peopleTestSchema, homer, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, homer, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, homer, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, homer, episodesTestSchema, ep4),
				concatRows(peopleTestSchema, marge, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, marge, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, marge, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, marge, episodesTestSchema, ep4),
				concatRows(peopleTestSchema, bart, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, bart, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, bart, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, bart, episodesTestSchema, ep4),
				concatRows(peopleTestSchema, lisa, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, lisa, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, lisa, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, lisa, episodesTestSchema, ep4),
				concatRows(peopleTestSchema, moe, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, moe, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, moe, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, moe, episodesTestSchema, ep4),
				concatRows(peopleTestSchema, barney, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, barney, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, barney, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, barney, episodesTestSchema, ep4),
			),
			expectedSchema: compressSchemas(peopleTestSchema, episodesTestSchema),
		},
		{
			name:  "Test natural join with where clause",
			query: `select * from people p, episodes e where e.id = p.id`,
			expectedRows: rs(
				concatRows(peopleTestSchema, marge, episodesTestSchema, ep1),
				concatRows(peopleTestSchema, bart, episodesTestSchema, ep2),
				concatRows(peopleTestSchema, lisa, episodesTestSchema, ep3),
				concatRows(peopleTestSchema, moe, episodesTestSchema, ep4),
			),
			expectedSchema: compressSchemas(peopleTestSchema, episodesTestSchema),
		},
		{
			name:  "Test natural join with where clause, select subset of columns",
			query: `select e.id, p.id, e.name, p.first, p.last from people p, episodes e where e.id = p.id`,
			expectedRows: rs(
				newResultSetRow(types.Int(1), types.Int(1), types.String("Simpsons Roasting On an Open Fire"), types.String("Marge"), types.String("Simpson")),
				newResultSetRow(types.Int(2), types.Int(2), types.String("Bart the Genius"), types.String("Bart"), types.String("Simpson")),
				newResultSetRow(types.Int(3), types.Int(3), types.String("Homer's Odyssey"), types.String("Lisa"), types.String("Simpson")),
				newResultSetRow(types.Int(4), types.Int(4), types.String("There's No Disgrace Like Home"), types.String("Moe"), types.String("Szyslak")),
			),
			expectedSchema: newResultSetSchema("id", types.IntKind, "id", types.IntKind,
				"name", types.StringKind, "first", types.StringKind, "last", types.StringKind),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot()

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedRows != nil && tt.expectedSchema == nil {
				assert.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
				t.FailNow()
			}

			rows, sch, err := ExecuteSelect(root, s)
			if err != nil {
				assert.True(t, tt.expectedErr, err.Error())
			} else {
				assert.False(t, tt.expectedErr, "unexpected error")
			}
			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
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

// Creates a new row for a result set specified by the given values
func newResultSetRow(colVals ...types.Value) row.Row {

	taggedVals := make(row.TaggedValues)
	cols := make([]schema.Column, len(colVals))
	for i := 0; i < len(colVals); i++ {
		taggedVals[uint64(i)] = colVals[i]
		nomsKind := colVals[i].Kind()
		cols[i] = schema.NewColumn(fmt.Sprintf("%v", i), uint64(i), nomsKind, false)
	}

	collection, err := schema.NewColCollection(cols...)
	if err != nil {
		panic("unexpected error " + err.Error())
	}
	sch := schema.UnkeyedSchemaFromCols(collection)

	return row.New(sch, taggedVals)
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
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema:  compressSchema(peopleTestSchema),
		},
		{
			name:  "Test select columns",
			query: "select age, id from people",
			expectedNumRows: len([]row.Row{homer, marge, bart, lisa, moe, barney}),
			expectedSchema: compressSchema(peopleTestSchema, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot()

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			p, statement, _ := BuildSelectQueryPipeline(root, s)
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
			assert.Equal(t, tt.expectedSchema, statement.ResultSetSchema.Schema())
		})
	}
}