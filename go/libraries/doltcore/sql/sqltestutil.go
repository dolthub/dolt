package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/stretchr/testify/assert"
	"math"
	"reflect"
	"testing"
)

// This file collects useful test table definitions and functions for SQL tests to use. It primarily defines a table
// name, schema, and some sample rows to use in tests, as well as functions for creating and seeding a test database,
// transforming row results, and so on.

const (
	idTag = iota
	firstTag
	lastTag
	isMarriedTag
	ageTag
	emptyTag
	ratingTag
	uuidTag
	numEpisodesTag
)

const (
	episodeIdTag = iota
	epNameTag
	epAirDateTag
	epRatingTag
)

var peopleTestSchema = createPeopleTestSchema()
var untypedPeopleSch = untyped.UntypeUnkeySchema(peopleTestSchema)
var peopleTableName = "people"

var episodesTestSchema = createEpisodesTestSchema()
var untypedEpisodesSch = untyped.UntypeUnkeySchema(episodesTestSchema)
var episodesTableName = "episodes"

func createSchema(columns... schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.SchemaFromCols(colColl)
}

func createPeopleTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.IntKind, false),
//		schema.NewColumn("empty", emptyTag, types.IntKind, false),
		schema.NewColumn("rating", ratingTag, types.FloatKind, false),
		schema.NewColumn("uuid", uuidTag, types.UUIDKind, false),
		schema.NewColumn("num_episodes", numEpisodesTag, types.UintKind, false),
	)
	return schema.SchemaFromCols(colColl)
}

func createEpisodesTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", episodeIdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", epNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("air_date", epAirDateTag, types.IntKind, false),
		schema.NewColumn("rating", epRatingTag, types.FloatKind, false),
	)
	return schema.SchemaFromCols(colColl)
}

func newPeopleRow(id int, first, last string, isMarried bool, age int, rating float32) row.Row {
	vals := row.TaggedValues{
		idTag: types.Int(id),
		firstTag: types.String(first),
		lastTag: types.String(last),
		isMarriedTag: types.Bool(isMarried),
		ageTag: types.Int(age),
		ratingTag: types.Float(rating),
	}

	return row.New(peopleTestSchema, vals)
}

func newEpsRow(id int, name string, airdate int, rating float32) row.Row {
	vals := row.TaggedValues{
		episodeIdTag: types.Int(id),
		epNameTag: types.String(name),
		epAirDateTag: types.Int(airdate),
		epRatingTag: types.Float(rating),
	}

	return row.New(episodesTestSchema, vals)
}

// Most rows don't have these optional fields set, as they aren't needed for basic testing
func newPeopleRowWithOptionalFields(id int, first, last string, isMarried bool, age int, rating float32, uid uuid.UUID, numEpisodes uint64) row.Row {
	vals := row.TaggedValues{
		idTag: types.Int(id),
		firstTag: types.String(first),
		lastTag: types.String(last),
		isMarriedTag: types.Bool(isMarried),
		ageTag: types.Int(age),
		ratingTag: types.Float(rating),
		uuidTag: types.UUID(uid),
		numEpisodesTag: types.Uint(numEpisodes),
	}

	return row.New(peopleTestSchema, vals)
}

// Default set of rows to use for sql tests
var homer = newPeopleRow(0, "Homer", "Simpson", true, 40, 8.5)
var marge = newPeopleRow(1, "Marge", "Simpson", true, 38, 8)
var bart = newPeopleRow(2, "Bart", "Simpson", false, 10, 9)
var lisa = newPeopleRow(3, "Lisa", "Simpson", false, 8, 10)
var moe = newPeopleRow(4, "Moe", "Szyslak", false, 48, 6.5)
var barney = newPeopleRow(5, "Barney", "Gumble", false, 40, 4)
var allPeopleRows = rs(homer, marge, bart, lisa, moe, barney)

var ep1 = newEpsRow(1, "Simpsons Roasting On an Open Fire", 629953200, 8.0)
var ep2 = newEpsRow(2, "Bart the Genius", 632372400, 9.0)
var ep3 = newEpsRow(3, "Homer's Odyssey", 632977200, 7.0)
var ep4 = newEpsRow(4, "There's No Disgrace Like Home", 633582000, 8.5)
var allEpsRows = rs(ep1, ep2, ep3, ep4)

func rs(rows... row.Row) []row.Row {
	return rows
}

// Returns the index of the first row in the list that has the same primary key as the one given, or -1 otherwise.
func findRowIndex(find row.Row, rows []row.Row) int {
	verifyIdx := -1
	for i, updatedRow := range rows {
		rowId, _ := find.GetColVal(idTag)
		updatedId, _ := updatedRow.GetColVal(idTag)
		if rowId.Equals(updatedId) {
			verifyIdx = i
			break
		}
	}
	return verifyIdx
}

// Compares two noms Floats for approximate equality
var floatComparer = cmp.Comparer(func(x, y types.Float) bool {
	return math.Abs(float64(x) - float64(y)) < .001
})

// Returns a subset of the schema given
func subsetSchema(sch schema.Schema, colNames ...string) schema.Schema {
	srcColls := sch.GetAllCols()

	if len(colNames) > 0 {
		var cols []schema.Column
		for _, name := range colNames {
			if col, ok := srcColls.GetByName(name); !ok {
				panic("Unrecognized name " + name)
			} else {
				cols = append(cols, col)
			}
		}
		colColl, _ := schema.NewColCollection(cols...)
		sch := schema.UnkeyedSchemaFromCols(colColl)
		return sch
	}

	return schema.UnkeyedSchemaFromCols(srcColls)
}

// Mutates the row given with pairs of {tag,value} given in the varargs param. Converts built-in types to noms types.
func mutateRow(r row.Row, tagsAndVals ...interface{}) row.Row {
	if len(tagsAndVals) % 2 != 0 {
		panic("expected pairs of tags and values")
	}

	var mutated row.Row = r
	var err error

	for i := 0; i < len(tagsAndVals); i += 2{
		tag := tagsAndVals[i].(int)
		val := tagsAndVals[i+1]
		var nomsVal types.Value
		if val != nil {
			switch v := val.(type) {
			case uint64:
				nomsVal = types.Uint(v)
			case int:
				nomsVal = types.Int(v)
			case int32:
				nomsVal = types.Int(v)
			case int64:
				nomsVal = types.Int(v)
			case float32:
				nomsVal = types.Float(v)
			case float64:
				nomsVal = types.Float(v)
			case string:
				nomsVal = types.String(v)
			case uuid.UUID:
				nomsVal = types.UUID(v)
			case bool:
				nomsVal = types.Bool(v)
			default:
				panic("Unhandled type " + reflect.TypeOf(val).String())
			}
		} else {
			nomsVal = nil
		}

		mutated, err = mutated.SetColVal(uint64(tag), nomsVal, peopleTestSchema)
		if err != nil {
			panic(err.Error())
		}
	}

	return mutated
}

// Converts the rows given from the source schema give to the destination schema given, using a simple tag mapping.
func convertRows(t *testing.T, rows []row.Row, srcSchema, destSchema schema.Schema) []row.Row {
	// Zero typing makes the nil slice and the empty slice appear equal to most functions, but they are semantically
	// distinct.
	if rows == nil {
		return nil
	}

	result := make([]row.Row, 0, len(rows))
	for _, r := range rows {
		result = append(result, convertRow(t, r, srcSchema, destSchema))
	}
	return result
}

// Converts the row given from the source schema given to the destination schema given using a simple tag mapping.
func convertRow(t *testing.T, r row.Row, sch, destSchema schema.Schema) row.Row {
	fieldMapping, _ := rowconv.TagMapping(sch, destSchema)

	rConv, _ := rowconv.NewRowConverter(fieldMapping)
	untyped, err := rConv.Convert(r)
	assert.Nil(t, err, "failed to untyped row to untyped")
	return untyped
}

func createTestTable(dEnv *env.DoltEnv, t *testing.T, tableName string, sch schema.Schema, rs... row.Row) {
	imt := table.NewInMemTable(sch)

	for _, r := range rs {
		imt.AppendRow(r)
	}

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(rd, wr, false)
	rd.Close()
	wr.Close()

	assert.Nil(t, err, "Failed to seed initial data")

	err = dEnv.PutTableToWorking(*wr.GetMap(), wr.GetSchema(), tableName)
	assert.Nil(t, err,"Unable to put initial value of table in in mem noms db")
}

// Creates a test database with the test data set in it
func createTestDatabase(dEnv *env.DoltEnv, t *testing.T) {
	createTestTable(dEnv, t, peopleTableName, peopleTestSchema, allPeopleRows...)
	createTestTable(dEnv, t, episodesTableName, episodesTestSchema, allEpsRows...)
}
