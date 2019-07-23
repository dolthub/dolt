package doltdb

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestRowData(vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	rows := make([]row.Row, 4)
	rows[0] = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id0), firstTag: types.String("bill"), lastTag: types.String("billerson"), ageTag: types.Uint(53)})
	rows[1] = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id1), firstTag: types.String("eric"), lastTag: types.String("ericson"), isMarriedTag: types.Bool(true), ageTag: types.Uint(21)})
	rows[2] = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id2), firstTag: types.String("john"), lastTag: types.String("johnson"), isMarriedTag: types.Bool(false), ageTag: types.Uint(53)})
	rows[3] = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id3), firstTag: types.String("robert"), lastTag: types.String("robertson"), ageTag: types.Uint(36)})

	ed := types.NewMap(context.Background(), vrw).Edit()
	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	return ed.Map(context.Background()), rows
}

func createTestTable(vrw types.ValueReadWriter, tSchema schema.Schema, rowData types.Map) (*Table, error) {
	schemaVal, err := encoding.MarshalAsNomsValue(context.Background(), vrw, tSchema)

	if err != nil {
		return nil, err
	}

	tbl := NewTable(context.Background(), vrw, schemaVal, rowData)

	return tbl, nil
}

func TestTables(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)

	tSchema := createTestSchema()
	rowData, rows := createTestRowData(db, tSchema)
	tbl, err := createTestTable(db, tSchema, rowData)

	if err != nil {
		t.Fatal("Failed to create table.")
	}

	//unmarshalledSchema := tbl.GetSchema()

	//if !tSchema.Equals(unmarshalledSchema) {
	//	t.Error("Schema has changed between writing and reading it back")
	//}

	badUUID, _ := uuid.NewRandom()
	ids := []types.Value{types.UUID(id0), types.UUID(id1), types.UUID(id2), types.UUID(id3), types.UUID(badUUID)}

	readRow0, ok := tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: ids[0]}, tSchema)

	if !ok {
		t.Error("Could not find row 0 in table")
	} else if !row.AreEqual(readRow0, rows[0], tSchema) {
		t.Error(row.Fmt(context.Background(), readRow0, tSchema), "!=", row.Fmt(context.Background(), rows[0], tSchema))
	}

	_, ok = tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: types.UUID(badUUID)}, tSchema)

	if ok {
		t.Error("GetRow should have returned false.")
	}

	idItr := SingleColPKItr(types.Format_7_18, idTag, ids)
	readRows, missing := tbl.GetRows(context.Background(), idItr, -1, tSchema)

	if len(readRows) != len(rows) {
		t.Error("Did not find all the expected rows")
	} else if len(missing) != 1 {
		t.Error("Expected one missing row for badUUID")
	}

	for i, r := range rows {
		if !row.AreEqual(r, readRows[i], tSchema) {
			t.Error(row.Fmt(context.Background(), readRows[i], tSchema), "!=", row.Fmt(context.Background(), r, tSchema))
		}
	}
}
