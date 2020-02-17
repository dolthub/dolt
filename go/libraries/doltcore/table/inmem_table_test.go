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

package table

import (
	"context"
	"io"
	"testing"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	nameTag uint64 = iota
	ageTag
	titleTag
	greatTag
)

var fields, _ = schema.NewColCollection(
	schema.Column{Name: "name", Tag: nameTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	schema.Column{Name: "age", Tag: ageTag, Kind: types.UintKind, IsPartOfPK: true, TypeInfo: typeinfo.Uint64Type, Constraints: nil},
	schema.Column{Name: "title", Tag: titleTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	schema.Column{Name: "is_great", Tag: greatTag, Kind: types.BoolKind, IsPartOfPK: true, TypeInfo: typeinfo.BoolType, Constraints: nil},
)

var rowSch = schema.SchemaFromCols(fields)

func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

// These are in noms-key-sorted order, since InMemoryTable.AppendRow sorts its rows. This should probably be done
// programatically instead of hard-coded.
var rows = []row.Row{
	mustRow(row.New(types.Format_7_18, rowSch, row.TaggedValues{
		nameTag:  types.String("Bill Billerson"),
		ageTag:   types.Uint(32),
		titleTag: types.String("Senior Dufus"),
		greatTag: types.Bool(true),
	})),
	mustRow(row.New(types.Format_7_18, rowSch, row.TaggedValues{
		nameTag:  types.String("John Johnson"),
		ageTag:   types.Uint(21),
		titleTag: types.String("Intern Dufus"),
		greatTag: types.Bool(true),
	})),
	mustRow(row.New(types.Format_7_18, rowSch, row.TaggedValues{
		nameTag:  types.String("Rob Robertson"),
		ageTag:   types.Uint(25),
		titleTag: types.String("Dufus"),
		greatTag: types.Bool(false),
	})),
}

func TestInMemTable(t *testing.T) {
	imt := NewInMemTable(rowSch)

	func() {
		var wr TableWriteCloser
		wr = NewInMemTableWriter(imt)
		defer wr.Close(context.Background())

		for _, row := range rows {
			err := wr.WriteRow(context.Background(), row)

			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()

	func() {
		var r TableReadCloser
		r = NewInMemTableReader(imt)
		defer r.Close(context.Background())

		for _, expectedRow := range rows {
			actualRow, err := r.ReadRow(context.Background())

			if err != nil {
				t.Error("Unexpected read error")
			} else if !row.AreEqual(expectedRow, actualRow, rowSch) {
				t.Error("Unexpected row value")
			}
		}

		_, err := r.ReadRow(context.Background())

		if err != io.EOF {
			t.Error("Should have reached the end.")
		}
	}()
}

func TestPipeRows(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)
	imtt2 := NewInMemTable(rowSch)

	var err error
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close(context.Background())
		wr := NewInMemTableWriter(imtt2)
		defer wr.Close(context.Background())
		_, _, err = PipeRows(context.Background(), rd, wr, false)
	}()

	if err != nil {
		t.Error("Error piping rows from reader to writer", err)
	}

	if imt.NumRows() != imtt2.NumRows() {
		t.Error("Row counts should match")
	}

	for i := 0; i < imt.NumRows(); i++ {
		r1, err1 := imt.GetRow(i)
		r2, err2 := imtt2.GetRow(i)

		if err1 != nil || err2 != nil {
			t.Fatal("Couldn't Get row.")
		}

		if !row.AreEqual(r1, r2, rowSch) {
			t.Error("Rows should be the same.", row.Fmt(context.Background(), r1, rowSch), "!=", row.Fmt(context.Background(), r2, rowSch))
		}
	}
}

func TestReadAllRows(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)

	var err error
	var numBad int
	var results []row.Row
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close(context.Background())
		results, numBad, err = ReadAllRows(context.Background(), rd, true)
	}()

	if err != nil {
		t.Fatal("Error reading rows")
	}

	if len(rows) != len(results) {
		t.Error("Unexpected count.")
	}

	if numBad != 0 {
		t.Error("Unexpected BadRow Count")
	}

	for i := 0; i < len(rows); i++ {
		if !row.AreEqual(rows[i], results[i], rowSch) {
			t.Error(row.Fmt(context.Background(), rows[i], rowSch), "!=", row.Fmt(context.Background(), results[i], rowSch))
		}
	}
}

/*
func TestReadAllRowsToMap(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)
	greatIndex := rowSch.GetFieldIndex("is_great")

	var err error
	var numBad int
	var results map[types.Value][]row.Row
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close()
		results, numBad, err = ReadAllRowsToMap(rd, greatIndex, true)
	}()

	if err != nil {
		t.Fatal("Error reading rows")
	}

	if numBad != 0 {
		t.Error("Unexpected BadRow Count")
	}

	if len(results) != 2 {
		t.Error("Unexpected count.")
	}

	if len(results[types.Bool(true)]) != 2 || len(results[types.Bool(false)]) != 1 {
		t.Error("Unexpected count for one or more values of is_great")
	}

	for _, great := range []types.Bool{types.Bool(true), types.Bool(false)} {
		for i, j := 0, 0; i < len(rows); i++ {
			rowIsGreat, _ := rows[i].CurrData().GetField(greatIndex)

			if rowIsGreat == great {
				if !RowsEqualIgnoringSchema(rows[i], results[great][j]) {
					t.Error(RowFmt(rows[i]), "!=", RowFmt(results[great][j]))
				}
				j++
			}
		}
	}
}
*/
