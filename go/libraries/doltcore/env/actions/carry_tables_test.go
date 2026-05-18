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

// collidingTag is used for both bar.code and users.name to engineer a tag collision that
// CarryUncommittedTables must resolve via a retag.
const (
	collidingTag  uint64 = 9815
	barCodeTag           = collidingTag
	usersNameTag         = collidingTag
	postsTitleTag uint64 = 13593
)

// putFK installs a single-entry foreign key collection onto |root| and returns the updated root.
func putFK(t *testing.T, ctx context.Context, root doltdb.RootValue, fk doltdb.ForeignKey) doltdb.RootValue {
	t.Helper()
	fks, err := doltdb.NewForeignKeyCollection(fk)
	require.NoError(t, err)
	updated, err := root.PutForeignKeyCollection(ctx, fks)
	require.NoError(t, err)
	return updated
}

// newEmptyRoot returns a context and the empty working root of a freshly initialised repo.
func newEmptyRoot(t *testing.T) (context.Context, doltdb.RootValue) {
	t.Helper()
	dEnv, _ := createTestEnv()
	ctx := context.Background()
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main"))
	emptyRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	return ctx, emptyRoot
}

// singleColPKSchema builds a one-column primary key schema using |colName| and |tag|.
func singleColPKSchema(t *testing.T, colName string, tag uint64) schema.Schema {
	t.Helper()
	col := schema.NewColumn(colName, tag, types.StringKind, true, schema.NotNullConstraint{})
	sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
	require.NoError(t, err)
	require.NoError(t, sch.SetPkOrdinals([]int{0}))
	return sch
}

// TestCarryUncommittedTablesDoesNotDuplicatePreexistingFK verifies that CarryUncommittedTables does not return an error
// when the target foreign key collection already contains a key being carried.
func TestCarryUncommittedTablesDoesNotDuplicatePreexistingFK(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	// Use non-colliding tags so no retag happens. This isolates the duplicate-FK guard.
	const parentTag uint64 = 50001
	const childTag uint64 = 50002

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "id", parentTag))
	require.NoError(t, err)

	working := emptyRoot
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "child"}, singleColPKSchema(t, "pid", childTag))
	require.NoError(t, err)

	fk := doltdb.ForeignKey{
		Name:                   "fk_pre",
		TableName:              doltdb.TableName{Name: "child"},
		TableColumns:           []uint64{childTag},
		ReferencedTableName:    doltdb.TableName{Name: "parent"},
		ReferencedTableColumns: []uint64{parentTag},
	}
	working = putFK(t, ctx, working, fk)
	// Simulate the FK pre-merge that RootsForBranch performs before calling CarryUncommittedTables.
	target = putFK(t, ctx, target, fk)

	result, err := CarryUncommittedTables(ctx, working, emptyRoot, target, CarryAll)
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

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "id", parentTagOnTarget))
	require.NoError(t, err)

	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "id", parentTagOnSource))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "child"}, singleColPKSchema(t, "pid", childTag))
	require.NoError(t, err)

	working = putFK(t, ctx, working, doltdb.ForeignKey{
		Name:                   "fk_cross",
		TableName:              doltdb.TableName{Name: "child"},
		TableColumns:           []uint64{childTag},
		ReferencedTableName:    doltdb.TableName{Name: "parent"},
		ReferencedTableColumns: []uint64{parentTagOnSource},
	})

	// |baseline| is target so only the child table is carried. The parent table already exists on
	// target and is filtered out of the carry candidates.
	result, err := CarryUncommittedTables(ctx, working, target, target, CarryAll)
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

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPKSchema(t, "code", barCodeTag))
	require.NoError(t, err)

	// Source has bar (so the foreign key can reference it), plus untracked users and posts.
	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPKSchema(t, "code", barCodeTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "users"}, withUniqueIndex(singleColPKSchema(t, "name", usersNameTag), "idx_name", usersNameTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "posts"}, singleColPKSchema(t, "title", postsTitleTag))
	require.NoError(t, err)

	working = putFK(t, ctx, working, doltdb.ForeignKey{
		Name:                   "fk_bar",
		TableName:              doltdb.TableName{Name: "users"},
		TableColumns:           []uint64{usersNameTag},
		ReferencedTableName:    doltdb.TableName{Name: "bar"},
		ReferencedTableColumns: []uint64{barCodeTag},
	})

	// |baseline| is target so bar is treated as tracked on source and only users and posts are carried.
	result, err := CarryUncommittedTables(ctx, working, target, target, CarryAll)
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

	t.Run("retags colliding column", func(t *testing.T) {
		tbl, ok, err := result.GetTable(ctx, doltdb.TableName{Name: "users"})
		require.NoError(t, err)
		require.True(t, ok, "users table must be present in the result")
		sch, err := tbl.GetSchema(ctx)
		require.NoError(t, err)
		col, ok := sch.GetAllCols().GetByName("name")
		require.True(t, ok)
		require.NotEqual(t, barCodeTag, col.Tag, "users.name must be retagged to avoid colliding with bar.code")

		idx, ok := sch.Indexes().GetByNameCaseInsensitive("idx_name")
		require.True(t, ok, "idx_name must survive the retag")
		require.Equal(t, []uint64{col.Tag}, idx.IndexedColumnTags(), "idx_name must reference the retagged column, not the old tag")
		require.True(t, idx.IsUnique(), "idx_name is unique and must remain so after retag")
		require.True(t, idx.IsUserDefined(), "idx_name is user-defined and must remain so after retag")
	})

	t.Run("FK child column retagged, parent column unchanged", func(t *testing.T) {
		// users.name and bar.code share collidingTag so users.name is retagged. bar.code is committed
		// on the target and keeps its tag, so only the child side of the FK changes.
		usersTbl, ok, err := result.GetTable(ctx, doltdb.TableName{Name: "users"})
		require.NoError(t, err)
		require.True(t, ok)
		usersSch, err := usersTbl.GetSchema(ctx)
		require.NoError(t, err)
		nameCol, ok := usersSch.GetAllCols().GetByName("name")
		require.True(t, ok)

		resultFks, err := result.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		got, ok := resultFks.GetByNameCaseInsensitive("fk_bar", doltdb.TableName{Name: "users"})
		require.True(t, ok, "fk_bar must be present in result FK collection")
		require.Equal(t, []uint64{nameCol.Tag}, got.TableColumns,
			"child column must follow the retagged users.name tag")
		require.Equal(t, []uint64{barCodeTag}, got.ReferencedTableColumns,
			"committed parent column tag must not change")
	})
}
