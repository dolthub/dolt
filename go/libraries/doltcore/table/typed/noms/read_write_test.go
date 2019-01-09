package noms

import (
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"testing"
)

var sch = schema.NewSchema([]*schema.Field{
	schema.NewField("id", types.UUIDKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("age", types.UintKind, true),
	schema.NewField("title", types.StringKind, false),
})

func init() {
	sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
}

var uuids = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var ages = []uint{32, 25, 21}
var titles = []string{"Senior Dufus", "Dufus", ""}

var updatedIndices = []bool{false, true, true}
var updatedAges = []uint{0, 26, 20}

func createRows(onlyUpdated, updatedAge bool) []*table.Row {
	rows := make([]*table.Row, 0, len(names))
	for i := 0; i < len(names); i++ {
		if !onlyUpdated || updatedIndices[i] {
			age := ages[i]
			if updatedAge && updatedIndices[i] {
				age = updatedAges[i]
			}

			rowValMap := map[string]types.Value{
				"id":    types.UUID(uuids[i]),
				"name":  types.String(names[i]),
				"age":   types.Uint(age),
				"title": types.String(titles[i]),
			}
			rows = append(rows, table.NewRow(table.RowDataFromValMap(sch, rowValMap)))
		}
	}

	return rows
}

func TestReadWrite(t *testing.T) {
	dbSPec, _ := spec.ForDatabase("mem")
	db := dbSPec.GetDatabase()

	rows := createRows(false, false)

	initialMapVal := testNomsMapCreator(t, db, rows)
	testReadAndCompare(t, initialMapVal, rows)

	updatedRows := createRows(true, true)
	updatedMap := testNomsMapUpdate(t, db, initialMapVal, updatedRows)

	expectedRows := createRows(false, true)
	testReadAndCompare(t, updatedMap, expectedRows)
}

func testNomsMapCreator(t *testing.T, vrw types.ValueReadWriter, rows []*table.Row) *types.Map {
	mc := NewNomsMapCreator(vrw, sch)
	return testNomsWriteCloser(t, mc, rows)
}

func testNomsMapUpdate(t *testing.T, vrw types.ValueReadWriter, initialMapVal *types.Map, rows []*table.Row) *types.Map {
	mu := NewNomsMapUpdater(vrw, *initialMapVal, sch)
	return testNomsWriteCloser(t, mu, rows)
}

func testNomsWriteCloser(t *testing.T, nwc NomsMapWriteCloser, rows []*table.Row) *types.Map {
	for _, row := range rows {
		err := nwc.WriteRow(row)

		if err != nil {
			t.Error("Failed to write row.", err)
		}
	}

	err := nwc.Close()

	if err != nil {
		t.Fatal("Failed to close writer")
	}

	err = nwc.Close()

	if err == nil {
		t.Error("Should give error for having already been closed")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Should panic when writing a row after closing.")
			}
		}()

		nwc.WriteRow(rows[0])
	}()

	mapVal := nwc.GetMap()

	if mapVal == nil {
		t.Fatal("Map should not be nil")
	}

	return mapVal
}

func testReadAndCompare(t *testing.T, initialMapVal *types.Map, expectedRows []*table.Row) {
	mr := NewNomsMapReader(*initialMapVal, sch)
	actualRows, numBad, err := table.ReadAllRows(mr, true)

	if err != nil {
		t.Fatal("Failed to read rows from map.")
	}

	if numBad != 0 {
		t.Error("Unexpectedly bad rows")
	}

	if len(actualRows) != len(expectedRows) {
		t.Fatal("Number of rows read does not match expectation")
	}

	for i := 0; i < len(expectedRows); i++ {
		if !table.RowsEqualIgnoringSchema(actualRows[i], expectedRows[i]) {
			t.Error(table.RowFmt(actualRows[i]), "!=", table.RowFmt(expectedRows[i]))
		}
	}
}
