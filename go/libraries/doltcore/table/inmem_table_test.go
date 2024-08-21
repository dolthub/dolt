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

package table

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	nameTag uint64 = iota
	ageTag
	titleTag
	greatTag
)

var fields = schema.NewColCollection(
	schema.Column{Name: "name", Tag: nameTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	schema.Column{Name: "age", Tag: ageTag, Kind: types.UintKind, IsPartOfPK: true, TypeInfo: typeinfo.Uint64Type, Constraints: nil},
	schema.Column{Name: "title", Tag: titleTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	schema.Column{Name: "is_great", Tag: greatTag, Kind: types.BoolKind, IsPartOfPK: true, TypeInfo: typeinfo.BoolType, Constraints: nil},
)

var rowSch = schema.MustSchemaFromCols(fields)

func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

// These are in noms-key-sorted order, since InMemoryTable.AppendRow sorts its rows. This should probably be done
// programmatically instead of hard-coded.
var rows = []row.Row{
	mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
		nameTag:  types.String("Bill Billerson"),
		ageTag:   types.Uint(32),
		titleTag: types.String("Senior Dufus"),
		greatTag: types.Bool(true),
	})),
	mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
		nameTag:  types.String("John Johnson"),
		ageTag:   types.Uint(21),
		titleTag: types.String("Intern Dufus"),
		greatTag: types.Bool(true),
	})),
	mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
		nameTag:  types.String("Rob Robertson"),
		ageTag:   types.Uint(25),
		titleTag: types.String("Dufus"),
		greatTag: types.Bool(false),
	})),
}

func TestInMemTable(t *testing.T) {
	vrw := types.NewMemoryValueStore()
	ctx := context.Background()
	imt := NewInMemTable(rowSch)

	func() {
		for _, r := range rows {
			err := imt.AppendRow(ctx, vrw, r)

			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()

	func() {
		var r ReadCloser
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
