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

package untyped

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func TestNewUntypedSchema(t *testing.T) {
	colNames := []string{"name", "city", "blurb"}
	nameToTag, sch := NewUntypedSchema(colNames...)

	if sch.GetAllCols().Size() != 3 {
		t.Error("Wrong column count")
	}

	i := 0
	_ = sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Name != colNames[i] {
			t.Error("Unexpected name")
		}

		if col.Kind != types.StringKind {
			t.Error("Unexpected kind")
		}

		if col.Constraints != nil {
			t.Error("Nothing should be required")
		}

		if !col.IsPartOfPK {
			t.Error("pk cols should be part of the pk")
		}

		i++
		return false, nil
	})
	assert.Equal(t, 1, i, "Exactly one PK column expected")

	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Name != colNames[i] {
			t.Error("Unexpected name")
		}

		if col.Kind != types.StringKind {
			t.Error("Unexpected kind")
		}

		if col.Constraints != nil {
			t.Error("Nothing should be required")
		}

		i++
		return false, nil
	})

	name := "Billy Bob"
	city := "Fargo"
	blurb := "Billy Bob is a scholar."
	r, err := NewRowFromStrings(types.Format_7_18, sch, []string{name, city, blurb})
	assert.NoError(t, err)

	nameVal, _ := r.GetColVal(nameToTag["name"])

	if nameVal.Kind() != types.StringKind || string(nameVal.(types.String)) != name {
		t.Error("Unexpected name")
	}

	cityVal, _ := r.GetColVal(nameToTag["city"])

	if cityVal.Kind() != types.StringKind || string(cityVal.(types.String)) != city {
		t.Error("Unexpected city")
	}

	blurbVal, _ := r.GetColVal(nameToTag["blurb"])

	if blurbVal.Kind() != types.StringKind || string(blurbVal.(types.String)) != blurb {
		t.Error("Unexpected blurb")
	}
}

func TestUntypedSchemaUnion(t *testing.T) {
	cols := []schema.Column{
		schema.NewColumn("a", 0, types.UUIDKind, true, "", false, "", schema.NotNullConstraint{}),
		schema.NewColumn("b", 1, types.IntKind, true, "", false, ""),
		schema.NewColumn("c", 2, types.UintKind, true, "", false, ""),
		schema.NewColumn("d", 3, types.StringKind, false, "", false, ""),
		schema.NewColumn("e", 4, types.BoolKind, false, "", false, ""),
	}

	untypedColColl, _ := schema.NewColCollection(
		schema.NewColumn("a", 0, types.StringKind, true, "", false, "", schema.NotNullConstraint{}),
		schema.NewColumn("b", 1, types.StringKind, true, "", false, ""),
		schema.NewColumn("c", 2, types.StringKind, true, "", false, ""),
		schema.NewColumn("d", 3, types.StringKind, false, "", false, ""),
		schema.NewColumn("e", 4, types.StringKind, false, "", false, ""))

	unequalColCollumn := cols[1]
	unequalColCollumn.Name = "bad"

	untypedSch := schema.SchemaFromCols(untypedColColl)

	tests := []struct {
		colsA     []schema.Column
		colsB     []schema.Column
		expectErr bool
	}{
		{cols[:2], cols[2:], false},
		{cols[:2], cols[1:], false},
		{cols[:2], []schema.Column{unequalColCollumn}, true},
	}

	for i, test := range tests {
		colCollA, _ := schema.NewColCollection(test.colsA...)
		colCollB, _ := schema.NewColCollection(test.colsB...)
		schA := schema.SchemaFromCols(colCollA)
		schB := schema.SchemaFromCols(colCollB)

		union, err := UntypedSchemaUnion(schA, schB)

		if (err != nil) != test.expectErr {
			t.Error(i, "expected err:", test.expectErr, "received err:", err != nil)
		} else if err == nil {
			assert.Equal(t, union, untypedSch)
		}
	}
}
