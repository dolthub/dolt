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

package diff

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func setupTable(t *testing.T, tableName string) (context.Context, *env.DoltEnv, types.Map) {
	ctx := context.Background()
	sch := dtestutils.TypedSchema
	rows := dtestutils.TypedRows
	dEnv := dtestutils.CreateTestEnv()
	_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)

	dtestutils.CreateTestTable(t, dEnv, tableName, sch, rows...)
	oldRoot, _ := dEnv.WorkingRoot(ctx)
	oldTable, _, err := oldRoot.GetTable(ctx, tableName)
	assert.NoError(t, err)

	oldRows, err := oldTable.GetRowData(ctx)
	assert.NoError(t, err)

	return ctx, dEnv, oldRows
}

func TestDiffSummary(t *testing.T) {
	tableName := "newTable"
	ctx, dEnv, oldRows := setupTable(t, tableName)

	id := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	r := dtestutils.NewTypedRow(id, "Jill Jillerson", 40, false, strPointer("Doctor"))
	newRoot, _ := dEnv.WorkingRoot(ctx)
	newRoot, _ = dtestutils.AddRowToRoot(dEnv, ctx, newRoot, tableName, r)
	newTable, _, err := newRoot.GetTable(ctx, tableName)
	assert.NoError(t, err)

	newRows, err := newTable.GetRowData(ctx)
	assert.NoError(t, err)

	expected := DifferenceSummary{
		RowsAdded:      1,
		RowsDeleted:    0,
		RowsModified:   0,
		RowsUnmodified: oldRows.Len(),
		CellsModified:  0,
		OldTableCount:  oldRows.Len(),
		NewTableCount:  newRows.Len(),
	}

	actual, err := DiffSummary(ctx, newRows, oldRows)
	assert.NoError(t, err)

	if !reflect.DeepEqual(actual, expected) {
		t.Error(actual, "!=", expected)
	}
}

func TestDiffSummaryNoChanges(t *testing.T) {
	tableName := "newTable"
	ctx, dEnv, oldRows := setupTable(t, tableName)

	newRoot, _ := dEnv.WorkingRoot(ctx)
	newTable, _, err := newRoot.GetTable(ctx, tableName)
	assert.NoError(t, err)

	newRows, err := newTable.GetRowData(ctx)
	assert.NoError(t, err)

	expected := DifferenceSummary{
		RowsAdded:      0,
		RowsDeleted:    0,
		RowsModified:   0,
		RowsUnmodified: oldRows.Len(),
		CellsModified:  0,
		OldTableCount:  oldRows.Len(),
		NewTableCount:  newRows.Len(),
	}
	actual, err := DiffSummary(ctx, newRows, oldRows)
	assert.NoError(t, err)

	if !reflect.DeepEqual(actual, expected) {
		t.Error(actual, "!=", expected)
	}
}

func TestDiffSummaryNoRows(t *testing.T) {
	tableName := "newTable"
	ctx := context.Background()
	sch := dtestutils.TypedSchema
	dEnv := dtestutils.CreateTestEnv()
	_ = dEnv.DoltDB.WriteEmptyRepo(ctx, name, email)

	dtestutils.CreateTestTable(t, dEnv, tableName, sch, []row.Row{}...)
	oldRoot, _ := dEnv.WorkingRoot(ctx)
	oldTable, _, err := oldRoot.GetTable(ctx, tableName)
	assert.NoError(t, err)

	oldRows, err := oldTable.GetRowData(ctx)
	assert.NoError(t, err)

	newRoot, _ := dEnv.WorkingRoot(ctx)
	newTable, _, err := newRoot.GetTable(ctx, tableName)
	assert.NoError(t, err)

	newRows, err := newTable.GetRowData(ctx)
	assert.NoError(t, err)

	expected := DifferenceSummary{}

	actual, err := DiffSummary(ctx, newRows, oldRows)
	assert.NoError(t, err)

	if !reflect.DeepEqual(actual, expected) {
		t.Error(actual, "!=", expected)
	}
}
