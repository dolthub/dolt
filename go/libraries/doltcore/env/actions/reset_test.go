// Copyright 2026 Dolthub, Inc.
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

package actions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	barCodeTag    uint64 = 9815
	usersNameTag  uint64 = 9815 // deliberate collision with barCodeTag
	postsTitleTag uint64 = 13593
)

// TestMoveUntrackedTables verifies that MoveUntrackedTables correctly copies untracked tables
// into the target root, retagging any column whose tag collides with an existing tag in the target.
func TestMoveUntrackedTables(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	dEnv, _ := createTestEnv()
	ctx := context.Background()
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main"))

	emptyRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	mkSch := func(colName string, tag uint64) schema.Schema {
		col := schema.NewColumn(colName, tag, types.StringKind, true, schema.NotNullConstraint{})
		sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
		require.NoError(t, err)
		require.NoError(t, sch.SetPkOrdinals([]int{0}))
		return sch
	}

	mkSchWithIndex := func(colName string, tag uint64, indexName string) schema.Schema {
		sch := mkSch(colName, tag)
		_, err := sch.Indexes().AddIndexByColTags(indexName, []uint64{tag}, nil, schema.IndexProperties{IsUserDefined: true, IsUnique: true})
		require.NoError(t, err)
		return sch
	}

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, mkSch("code", barCodeTag))
	require.NoError(t, err)

	working := emptyRoot
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "users"}, mkSchWithIndex("name", usersNameTag, "idx_name"))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "posts"}, mkSch("title", postsTitleTag))
	require.NoError(t, err)

	result, allRemaps, err := MoveUntrackedTables(ctx, working, emptyRoot, target)
	require.NoError(t, err)

	t.Run("preserves tag on non-collision", func(t *testing.T) {
		tbl, ok, err := result.GetTable(ctx, doltdb.TableName{Name: "posts"})
		require.NoError(t, err)
		require.True(t, ok, "posts table must be present in the result")
		sch, err := tbl.GetSchema(ctx)
		require.NoError(t, err)
		col, ok := sch.GetAllCols().GetByName("title")
		require.True(t, ok)
		require.Equal(t, postsTitleTag, col.Tag, "posts.title tag must not change when there is no collision")
	})

	t.Run("WithUpdatedColumnTags preserves schema metadata", func(t *testing.T) {
		col := schema.NewColumn("val", 100, types.StringKind, true, schema.NotNullConstraint{})
		sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
		require.NoError(t, err)
		require.NoError(t, sch.SetPkOrdinals([]int{0}))
		sch.SetCollation(schema.Collation_utf8mb4_general_ci)
		sch.SetComment("test comment")
		sch.SetTargetRowSize(4096)

		remapped, err := schema.WithUpdatedColumnTags(sch, map[uint64]uint64{100: 200})
		require.NoError(t, err)
		require.Equal(t, schema.Collation_utf8mb4_general_ci, remapped.GetCollation(), "collation must survive WithUpdatedColumnTags")
		require.Equal(t, "test comment", remapped.GetComment(), "comment must survive WithUpdatedColumnTags")
		require.Equal(t, uint16(4096), remapped.GetTargetRowSize(), "target row size must survive WithUpdatedColumnTags")
	})

	t.Run("retags colliding column", func(t *testing.T) {
		// AutoGenerateTag deterministically picks this tag when 9815 is already occupied.
		// The value is stable even though Go maps are iterated in random order each test run.
		const wantTag uint64 = 12204
		tbl, ok, err := result.GetTable(ctx, doltdb.TableName{Name: "users"})
		require.NoError(t, err)
		require.True(t, ok, "users table must be present in the result")
		sch, err := tbl.GetSchema(ctx)
		require.NoError(t, err)
		col, ok := sch.GetAllCols().GetByName("name")
		require.True(t, ok)
		require.Equal(t, wantTag, col.Tag, "users.name must be retagged to avoid colliding with bar.code")

		idx, ok := sch.Indexes().GetByNameCaseInsensitive("idx_name")
		require.True(t, ok, "idx_name must survive the retag")
		require.Equal(t, []uint64{wantTag}, idx.IndexedColumnTags(), "idx_name must reference the retagged column, not the old tag")
		require.True(t, idx.IsUnique(), "idx_name is unique and must remain so after retag")
		require.True(t, idx.IsUserDefined(), "idx_name is user-defined and must remain so after retag")
	})

	t.Run("ApplyForeignKeyTagRemaps patches child side only when parent is committed", func(t *testing.T) {
		// users.name and bar.code share tag 9815 so MoveUntrackedTables retagged users.name.
		// The FK links users.name to bar.code so only the child side should change.
		const wantTag uint64 = 12204

		fks, err := doltdb.NewForeignKeyCollection(doltdb.ForeignKey{
			Name:                   "fk_bar",
			TableName:              doltdb.TableName{Name: "users"},
			TableColumns:           []uint64{usersNameTag},
			ReferencedTableName:    doltdb.TableName{Name: "bar"},
			ReferencedTableColumns: []uint64{barCodeTag},
		})
		require.NoError(t, err)

		require.NoError(t, ApplyForeignKeyTagRemaps(fks, allRemaps))

		got, ok := fks.GetByNameCaseInsensitive("fk_bar", doltdb.TableName{Name: "users"})
		require.True(t, ok)
		require.Equal(t, []uint64{wantTag}, got.TableColumns,
			"child column must be updated to the retagged value")
		require.Equal(t, []uint64{barCodeTag}, got.ReferencedTableColumns,
			"committed parent column tag must not change")
	})
}
