// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doltdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestRowData(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	var err error
	rows := make([]row.Row, 4)
	rows[0], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id0), firstTag: types.String("bill"), lastTag: types.String("billerson"), ageTag: types.Uint(53)})
	assert.NoError(t, err)
	rows[1], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id1), firstTag: types.String("eric"), lastTag: types.String("ericson"), isMarriedTag: types.Bool(true), ageTag: types.Uint(21)})
	assert.NoError(t, err)
	rows[2], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id2), firstTag: types.String("john"), lastTag: types.String("johnson"), isMarriedTag: types.Bool(false), ageTag: types.Uint(53)})
	assert.NoError(t, err)
	rows[3], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id3), firstTag: types.String("robert"), lastTag: types.String("robertson"), ageTag: types.Uint(36)})
	assert.NoError(t, err)

	m, err := types.NewMap(context.Background(), vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = ed.Map(context.Background())
	assert.NoError(t, err)

	return m, rows
}

func createTestTable(vrw types.ValueReadWriter, tSchema schema.Schema, rowData types.Map) (*Table, error) {
	schemaVal, err := encoding.MarshalAsNomsValue(context.Background(), vrw, tSchema)

	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(context.Background(), vrw, schemaVal, rowData)

	return tbl, nil
}

func TestIsValidTableName(t *testing.T) {
	assert.True(t, IsValidTableName("a"))
	assert.True(t, IsValidTableName("a1"))
	assert.True(t, IsValidTableName("a1_b_c------1"))
	assert.True(t, IsValidTableName("Add-098234_lkjasdf0p98"))
	assert.False(t, IsValidTableName("1"))
	assert.False(t, IsValidTableName("-"))
	assert.False(t, IsValidTableName("-a"))
	assert.False(t, IsValidTableName("__a"))
	assert.False(t, IsValidTableName(""))
	assert.False(t, IsValidTableName("1a"))
	assert.False(t, IsValidTableName("a1-"))
	assert.False(t, IsValidTableName("ab!!c"))
}

func TestTables(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)

	tSchema := createTestSchema()
	rowData, rows := createTestRowData(t, db, tSchema)
	tbl, err := createTestTable(db, tSchema, rowData)
	assert.NoError(t, err)

	if err != nil {
		t.Fatal("Failed to create table.")
	}

	//unmarshalledSchema := tbl.GetSchema()

	//if !tSchema.Equals(unmarshalledSchema) {
	//	t.Error("Schema has changed between writing and reading it back")
	//}

	badUUID, _ := uuid.NewRandom()
	ids := []types.Value{types.UUID(id0), types.UUID(id1), types.UUID(id2), types.UUID(id3), types.UUID(badUUID)}

	readRow0, ok, err := tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: ids[0]}, tSchema)
	assert.NoError(t, err)

	if !ok {
		t.Error("Could not find row 0 in table")
	} else if !row.AreEqual(readRow0, rows[0], tSchema) {
		t.Error(row.Fmt(context.Background(), readRow0, tSchema), "!=", row.Fmt(context.Background(), rows[0], tSchema))
	}

	_, ok, err = tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: types.UUID(badUUID)}, tSchema)
	assert.NoError(t, err)

	if ok {
		t.Error("GetRow should have returned false.")
	}

	idItr := SingleColPKItr(types.Format_7_18, idTag, ids)
	readRows, missing, err := tbl.GetRows(context.Background(), idItr, -1, tSchema)
	assert.NoError(t, err)

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
