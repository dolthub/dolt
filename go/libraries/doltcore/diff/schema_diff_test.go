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

package diff

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func TestDiffSchemas(t *testing.T) {
	oldCols := []schema.Column{
		schema.NewColumn("unchanged", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("dropped", 1, types.StringKind, true),
		schema.NewColumn("renamed", 2, types.StringKind, false),
		schema.NewColumn("type_changed", 3, types.StringKind, false),
		schema.NewColumn("moved_to_pk", 4, types.StringKind, false),
		schema.NewColumn("contraint_added", 5, types.StringKind, false),
	}

	newCols := []schema.Column{
		schema.NewColumn("unchanged", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("renamed_new", 2, types.StringKind, false),
		schema.NewColumn("type_changed", 3, types.IntKind, false),
		schema.NewColumn("moved_to_pk", 4, types.StringKind, true),
		schema.NewColumn("contraint_added", 5, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("added", 6, types.StringKind, false),
	}

	oldColColl := schema.NewColCollection(oldCols...)
	newColColl := schema.NewColCollection(newCols...)

	oldSch, err := schema.SchemaFromCols(oldColColl)
	require.NoError(t, err)
	newSch, err := schema.SchemaFromCols(newColColl)
	require.NoError(t, err)
	diffs, _ := DiffSchColumns(oldSch, newSch)

	expected := map[uint64]ColumnDifference{
		0: {SchDiffNone, 0, &oldCols[0], &newCols[0]},
		1: {SchDiffRemoved, 1, &oldCols[1], nil},
		2: {SchDiffModified, 2, &oldCols[2], &newCols[1]},
		3: {SchDiffModified, 3, &oldCols[3], &newCols[2]},
		4: {SchDiffModified, 4, &oldCols[4], &newCols[3]},
		5: {SchDiffModified, 5, &oldCols[5], &newCols[4]},
		6: {SchDiffAdded, 6, nil, &newCols[5]},
	}

	if !reflect.DeepEqual(diffs, expected) {
		t.Error(diffs, "!=", expected)
	}
}
