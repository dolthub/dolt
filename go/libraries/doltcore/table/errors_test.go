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
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestBadRow(t *testing.T) {
	cols, _ := schema.NewColCollection(schema.NewColumn("id", 0, types.IntKind, true))
	sch := schema.SchemaFromCols(cols)
	emptyRow := row.New(types.Format_7_18, sch, row.TaggedValues{})

	err := NewBadRow(emptyRow, "details")

	if !IsBadRow(err) {
		t.Error("Should be a bad row error")
	}

	if !row.AreEqual(GetBadRowRow(err), emptyRow, sch) {
		t.Error("did not get back expected empty row")
	}

	if err.Error() != "details" {
		t.Error("unexpected details")
	}
}
