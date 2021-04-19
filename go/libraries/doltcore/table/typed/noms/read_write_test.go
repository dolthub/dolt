// Copyright 2019 Dolthub, Inc.
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

package noms

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	idCol       = "id"
	nameCol     = "name"
	ageCol      = "age"
	titleCol    = "title"
	idColTag    = 4
	nameColTag  = 3
	ageColTag   = 2
	titleColTag = 1
)

var colColl = schema.NewColCollection(
	schema.NewColumn(idCol, idColTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(nameCol, nameColTag, types.StringKind, false),
	schema.NewColumn(ageCol, ageColTag, types.UintKind, false),
	schema.NewColumn(titleCol, titleColTag, types.StringKind, false),
)
var sch = schema.MustSchemaFromCols(colColl)

var uuids = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var ages = []uint{32, 25, 21}
var titles = []string{"Senior Dufus", "Dufus", ""}

var updatedIndices = []bool{false, true, true}
var updatedAges = []uint{0, 26, 20}

func createRows(t *testing.T, onlyUpdated, updatedAge bool) []row.Row {
	rows := make([]row.Row, 0, len(names))
	for i := 0; i < len(names); i++ {
		if !onlyUpdated || updatedIndices[i] {
			age := ages[i]
			if updatedAge && updatedIndices[i] {
				age = updatedAges[i]
			}

			rowVals := row.TaggedValues{
				idColTag:    types.UUID(uuids[i]),
				nameColTag:  types.String(names[i]),
				ageColTag:   types.Uint(age),
				titleColTag: types.String(titles[i]),
			}

			r, err := row.New(types.Format_Default, sch, rowVals)
			assert.NoError(t, err)

			rows = append(rows, r)
		}
	}

	return rows
}

func TestReadWrite(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)

	rows := createRows(t, false, false)

	initialMapVal := testNomsMapCreator(t, db, rows)
	testReadAndCompare(t, initialMapVal, rows)

	updatedRows := createRows(t, true, true)
	updatedMap := testNomsMapUpdate(t, db, initialMapVal, updatedRows)

	expectedRows := createRows(t, false, true)
	testReadAndCompare(t, updatedMap, expectedRows)
}

func testNomsMapCreator(t *testing.T, vrw types.ValueReadWriter, rows []row.Row) types.Map {
	mc := NewNomsMapCreator(context.Background(), vrw, sch)

	for _, r := range rows {
		err := mc.WriteRow(context.Background(), r)

		if err != nil {
			t.Error("Failed to write row.", err)
		}
	}

	err := mc.Close(context.Background())

	if err != nil {
		t.Fatal("Failed to close writer")
	}

	err = mc.Close(context.Background())

	if err == nil {
		t.Error("Should give error for having already been closed")
	}

	err = mc.WriteRow(context.Background(), rows[0])

	if err == nil {
		t.Error("Should give error for writing after closing.")
	}

	return mc.GetMap()
}

func testNomsMapUpdate(t *testing.T, vrw types.ValueReadWriter, initialMapVal types.Map, rows []row.Row) types.Map {
	mu := NewNomsMapUpdater(context.Background(), vrw, initialMapVal, sch, nil)

	for _, r := range rows {
		err := mu.WriteRow(context.Background(), r)

		if err != nil {
			t.Error("Failed to write row.", err)
		}
	}

	err := mu.Close(context.Background())

	if err != nil {
		t.Fatal("Failed to close writer")
	}

	err = mu.Close(context.Background())

	if err == nil {
		t.Error("Should give error for having already been closed")
	}

	err = mu.WriteRow(context.Background(), rows[0])

	if err == nil {
		t.Error("Should give error for writing after closing.")
	}

	return mu.GetMap()
}

func testReadAndCompare(t *testing.T, initialMapVal types.Map, expectedRows []row.Row) {
	ctx := context.Background()
	mr, err := NewNomsMapReader(context.Background(), initialMapVal, sch)
	assert.NoError(t, err)

	var r row.Row
	var actualRows []row.Row
	for {
		r, err = mr.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		actualRows = append(actualRows, r)
	}
	assert.Equal(t, len(actualRows), len(expectedRows))

	for i := 0; i < len(expectedRows); i++ {
		if !row.AreEqual(actualRows[i], expectedRows[i], sch) {
			t.Error(row.Fmt(ctx, actualRows[i], sch), "!=", row.Fmt(context.Background(), expectedRows[i], sch))
		}
	}
}
