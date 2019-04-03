package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/stretchr/testify/assert"
	"testing"
)

// This file collects useful test table definitions and functions for SQL tests to use. It primarily defines a table
// name, schema, and some sample rows to use in tests, as well as functions for creating and seeding a test database,
// transforming row results, and so on.

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

func createSchema(columns... schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.SchemaFromCols(colColl)
}


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

// Default set of rows to use for sql tests
var homer = newRow(0, "Homer", "Simpson", true, 40)
var marge = newRow(1, "Marge", "Simpson", true, 38)
var bart = newRow(2, "Bart", "Simpson", false, 10)
var lisa = newRow(3, "Lisa", "Simpson", false, 8)
var moe = newRow(4, "Moe", "Szyslak", false, 48)
var barney = newRow(5, "Barney", "Gumble", false, 40)

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

// Creates a test database with the test data set in it
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
