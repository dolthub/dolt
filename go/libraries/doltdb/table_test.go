package doltdb

import (
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"testing"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestRowData(vrw types.ValueReadWriter, sch *schema.Schema) (types.Map, []*table.Row) {
	rows := make([]*table.Row, 4)
	rows[0] = table.NewRow(table.RowDataFromValMap(sch, map[string]types.Value{
		"id": types.UUID(id0), "first": types.String("bill"), "last": types.String("billerson"), "age": types.Uint(53)}))
	rows[1] = table.NewRow(table.RowDataFromValMap(sch, map[string]types.Value{
		"id": types.UUID(id1), "first": types.String("eric"), "last": types.String("ericson"), "is_married": types.Bool(true), "age": types.Uint(21)}))
	rows[2] = table.NewRow(table.RowDataFromValMap(sch, map[string]types.Value{
		"id": types.UUID(id2), "first": types.String("john"), "last": types.String("johnson"), "is_married": types.Bool(false), "age": types.Uint(53)}))
	rows[3] = table.NewRow(table.RowDataFromValMap(sch, map[string]types.Value{
		"id": types.UUID(id3), "first": types.String("robert"), "last": types.String("robertson"), "age": types.Uint(36)}))

	ed := types.NewMap(vrw).Edit()
	for _, row := range rows {
		ed = ed.Set(table.GetPKFromRow(row), table.GetNonPKFieldListFromRow(row, vrw))
	}

	return ed.Map(), rows
}

func createTestTable(vrw types.ValueReadWriter, tSchema *schema.Schema, rowData types.Map) (*Table, error) {
	schemaVal, err := noms.MarshalAsNomsValue(vrw, tSchema)

	if err != nil {
		return nil, err
	}

	tbl := NewTable(vrw, schemaVal, rowData)

	return tbl, nil
}

func TestTables(t *testing.T) {
	dbSPec, _ := spec.ForDatabase("mem")
	db := dbSPec.GetDatabase()

	tSchema := createTestSchema()
	rowData, rows := createTestRowData(db, tSchema)
	tbl, err := createTestTable(db, tSchema, rowData)

	if err != nil {
		t.Fatal("Failed to create table.")
	}

	unmarshalledSchema := tbl.GetSchema(db)

	if !tSchema.Equals(unmarshalledSchema) {
		t.Error("Schema has changed between writing and reading it back")
	}

	badUUID, _ := uuid.NewRandom()
	ids := []types.Value{types.UUID(id0), types.UUID(id1), types.UUID(id2), types.UUID(id3), types.UUID(badUUID)}

	readRow0, ok := tbl.GetRow(ids[0], tSchema)

	if !ok {
		t.Error("Could not find row 0 in table")
	} else if !table.RowsEqualIgnoringSchema(readRow0, rows[0]) {
		t.Error(table.RowFmt(readRow0), "!=", table.RowFmt(rows[0]))
	} else {
		t.Log("Rows equal:", table.RowFmt(readRow0))
	}

	_, ok = tbl.GetRow(types.UUID(badUUID), tSchema)

	if ok {
		t.Error("GetRow should have returned false.")
	}

	readRows, missing := tbl.GetRows(ValueSliceItr(ids), -1, tSchema)

	if len(readRows) != len(rows) {
		t.Error("Did not find all the expected rows")
	} else if len(missing) != 1 {
		t.Error("Expected one missing row for badUUID")
	}

	for i, row := range rows {
		if !table.RowsEqualIgnoringSchema(row, readRows[i]) {
			t.Error(table.RowFmt(readRows[i]), "!=", table.RowFmt(row))
		}
	}
}
