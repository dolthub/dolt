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

// newEmptyRoot returns a context and the empty working root of a freshly initialised repo.
func newEmptyRoot(t *testing.T) (context.Context, doltdb.RootValue) {
	dEnv, _ := createTestEnv()
	ctx := context.Background()
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main"))
	emptyRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	return ctx, emptyRoot
}

// singleColPkSchema builds a one-column primary key schema using |colName| and |tag|.
func singleColPkSchema(t *testing.T, colName string, tag uint64) schema.Schema {
	col := schema.NewColumn(colName, tag, types.StringKind, true, schema.NotNullConstraint{})
	sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
	require.NoError(t, err)
	require.NoError(t, sch.SetPkOrdinals([]int{0}))
	return sch
}

// TestCarryUncommittedTablesPreexistingFK verifies that CarryUncommittedTables does not return an error
// when the target foreign key collection already contains a key being carried.
func TestCarryUncommittedTablesPreexistingFK(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	// Use non-colliding tags so no retag happens. This isolates the duplicate-FK guard.
	const parentTag uint64 = 50001
	const childTag uint64 = 50002

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPkSchema(t, "id", parentTag))
	require.NoError(t, err)

	working := emptyRoot
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "child"}, singleColPkSchema(t, "pid", childTag))
	require.NoError(t, err)

	fks, err := doltdb.NewForeignKeyCollection(doltdb.ForeignKey{
		Name:                   "fk_pre",
		TableName:              doltdb.TableName{Name: "child"},
		TableColumns:           []uint64{childTag},
		ReferencedTableName:    doltdb.TableName{Name: "parent"},
		ReferencedTableColumns: []uint64{parentTag},
	})
	require.NoError(t, err)
	working, err = working.PutForeignKeyCollection(ctx, fks)
	require.NoError(t, err)

	// Simulate the FK pre-merge that RootsForBranch performs before calling CarryUncommittedTables.
	target, err = target.PutForeignKeyCollection(ctx, fks)
	require.NoError(t, err)

	result, err := CarryUncommittedTables(ctx, working, emptyRoot, target)
	require.NoError(t, err)

	resultFks, err := result.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	got, ok := resultFks.GetByNameCaseInsensitive("fk_pre", doltdb.TableName{Name: "child"})
	require.True(t, ok, "fk_pre must be present in result FK collection")
	require.Equal(t, []uint64{childTag}, got.TableColumns)
}

// TestCarryUncommittedTablesParentTagMismatch verifies that a foreign key carried from source onto
// target gets its referenced column tags rewritten to match the target parent schema when the
// same parent column has a different tag on source and target.
func TestCarryUncommittedTablesParentTagMismatch(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	const parentTagOnSource uint64 = 60001
	const parentTagOnTarget uint64 = 60002
	const childTag uint64 = 60003

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPkSchema(t, "id", parentTagOnTarget))
	require.NoError(t, err)

	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPkSchema(t, "id", parentTagOnSource))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "child"}, singleColPkSchema(t, "pid", childTag))
	require.NoError(t, err)

	fks, err := doltdb.NewForeignKeyCollection(doltdb.ForeignKey{
		Name:                   "fk_cross",
		TableName:              doltdb.TableName{Name: "child"},
		TableColumns:           []uint64{childTag},
		ReferencedTableName:    doltdb.TableName{Name: "parent"},
		ReferencedTableColumns: []uint64{parentTagOnSource},
	})
	require.NoError(t, err)
	working, err = working.PutForeignKeyCollection(ctx, fks)
	require.NoError(t, err)

	// sourceStaged is emptyRoot so child is untracked and parent (in working) is treated the same way.
	// parent already exists in target, so carryTables skips it and only child is carried.
	result, err := CarryUncommittedTables(ctx, working, target, target)
	require.NoError(t, err)

	resultFks, err := result.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	got, ok := resultFks.GetByNameCaseInsensitive("fk_cross", doltdb.TableName{Name: "child"})
	require.True(t, ok, "fk_cross must be present in result FK collection")
	require.Equal(t, []uint64{parentTagOnTarget}, got.ReferencedTableColumns,
		"referenced column tag must be rewritten to match target's parent schema")
	require.Equal(t, []uint64{childTag}, got.TableColumns,
		"child column tag must be unchanged because there was no collision")
}

// TestCarryUncommittedTables verifies that CarryUncommittedTables carries untracked tables into the
// target root, resolves tag collisions, and propagates foreign keys.
func TestCarryUncommittedTables(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	withUniqueIndex := func(sch schema.Schema, indexName string, tag uint64) schema.Schema {
		_, err := sch.Indexes().AddIndexByColTags(indexName, []uint64{tag}, nil, schema.IndexProperties{IsUserDefined: true, IsUnique: true})
		require.NoError(t, err)
		return sch
	}

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPkSchema(t, "code", barCodeTag))
	require.NoError(t, err)

	// Source has bar (so the foreign key can reference it), plus untracked users and posts.
	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPkSchema(t, "code", barCodeTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "users"}, withUniqueIndex(singleColPkSchema(t, "name", usersNameTag), "idx_name", usersNameTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "posts"}, singleColPkSchema(t, "title", postsTitleTag))
	require.NoError(t, err)

	fks, err := doltdb.NewForeignKeyCollection(doltdb.ForeignKey{
		Name:                   "fk_bar",
		TableName:              doltdb.TableName{Name: "users"},
		TableColumns:           []uint64{usersNameTag},
		ReferencedTableName:    doltdb.TableName{Name: "bar"},
		ReferencedTableColumns: []uint64{barCodeTag},
	})
	require.NoError(t, err)
	working, err = working.PutForeignKeyCollection(ctx, fks)
	require.NoError(t, err)

	// sourceStaged is target so bar is treated as already-tracked on source and only users and posts are carried.
	result, err := CarryUncommittedTables(ctx, working, target, target)
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

	t.Run("WithRemappedColumnTags preserves schema metadata", func(t *testing.T) {
		col := schema.NewColumn("val", 100, types.StringKind, true, schema.NotNullConstraint{})
		sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
		require.NoError(t, err)
		require.NoError(t, sch.SetPkOrdinals([]int{0}))
		sch.SetCollation(schema.Collation_utf8mb4_general_ci)
		sch.SetComment("test comment")
		sch.SetTargetRowSize(4096)

		remapped, err := schema.WithRemappedColumnTags(sch, map[uint64]uint64{100: 200})
		require.NoError(t, err)
		require.Equal(t, schema.Collation_utf8mb4_general_ci, remapped.GetCollation(), "collation must survive WithRemappedColumnTags")
		require.Equal(t, "test comment", remapped.GetComment(), "comment must survive WithRemappedColumnTags")
		require.Equal(t, uint16(4096), remapped.GetTargetRowSize(), "target row size must survive WithRemappedColumnTags")
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

	t.Run("FK child column retagged, parent column unchanged", func(t *testing.T) {
		// users.name and bar.code share tag 9815 so CarryUncommittedTables retagged users.name.
		// Only the child side of the FK changes because bar.code is committed and its tag is not affected by the carry.
		const wantTag uint64 = 12204
		resultFks, err := result.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		got, ok := resultFks.GetByNameCaseInsensitive("fk_bar", doltdb.TableName{Name: "users"})
		require.True(t, ok, "fk_bar must be present in result FK collection")
		require.Equal(t, []uint64{wantTag}, got.TableColumns,
			"child column must be updated to the retagged value")
		require.Equal(t, []uint64{barCodeTag}, got.ReferencedTableColumns,
			"committed parent column tag must not change")
	})
}
