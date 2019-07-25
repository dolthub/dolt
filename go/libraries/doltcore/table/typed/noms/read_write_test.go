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

package noms

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/store/types"
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

var colColl, _ = schema.NewColCollection(
	schema.NewColumn(idCol, idColTag, types.UUIDKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(nameCol, nameColTag, types.StringKind, false),
	schema.NewColumn(ageCol, ageColTag, types.UintKind, false),
	schema.NewColumn(titleCol, titleColTag, types.StringKind, false),
)
var sch = schema.SchemaFromCols(colColl)

var uuids = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var ages = []uint{32, 25, 21}
var titles = []string{"Senior Dufus", "Dufus", ""}

var updatedIndices = []bool{false, true, true}
var updatedAges = []uint{0, 26, 20}

func createRows(onlyUpdated, updatedAge bool) []row.Row {
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
			rows = append(rows, row.New(types.Format_7_18, sch, rowVals))
		}
	}

	return rows
}

func TestReadWrite(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)

	rows := createRows(false, false)

	initialMapVal := testNomsMapCreator(t, db, rows)
	testReadAndCompare(t, initialMapVal, rows)

	updatedRows := createRows(true, true)
	updatedMap := testNomsMapUpdate(t, db, initialMapVal, updatedRows)

	expectedRows := createRows(false, true)
	testReadAndCompare(t, updatedMap, expectedRows)
}

func testNomsMapCreator(t *testing.T, vrw types.ValueReadWriter, rows []row.Row) *types.Map {
	mc := NewNomsMapCreator(context.Background(), vrw, sch)
	return testNomsWriteCloser(t, mc, rows)
}

func testNomsMapUpdate(t *testing.T, vrw types.ValueReadWriter, initialMapVal *types.Map, rows []row.Row) *types.Map {
	mu := NewNomsMapUpdater(context.Background(), vrw, *initialMapVal, sch, nil)
	return testNomsWriteCloser(t, mu, rows)
}

func testNomsWriteCloser(t *testing.T, nwc NomsMapWriteCloser, rows []row.Row) *types.Map {
	for _, r := range rows {
		err := nwc.WriteRow(context.Background(), r)

		if err != nil {
			t.Error("Failed to write row.", err)
		}
	}

	err := nwc.Close(context.Background())

	if err != nil {
		t.Fatal("Failed to close writer")
	}

	err = nwc.Close(context.Background())

	if err == nil {
		t.Error("Should give error for having already been closed")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Should panic when writing a row after closing.")
			}
		}()

		nwc.WriteRow(context.Background(), rows[0])
	}()

	mapVal := nwc.GetMap()

	if mapVal == nil {
		t.Fatal("DestRef should not be nil")
	}

	return mapVal
}

func testReadAndCompare(t *testing.T, initialMapVal *types.Map, expectedRows []row.Row) {
	mr := NewNomsMapReader(context.Background(), *initialMapVal, sch)
	actualRows, numBad, err := table.ReadAllRows(context.Background(), mr, true)

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
		if !row.AreEqual(actualRows[i], expectedRows[i], sch) {
			t.Error(row.Fmt(context.Background(), actualRows[i], sch), "!=", row.Fmt(context.Background(), expectedRows[i], sch))
		}
	}
}
