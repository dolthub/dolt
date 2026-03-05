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
	r, err := NewRowFromStrings(types.Format_Default, sch, []string{name, city, blurb})
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
