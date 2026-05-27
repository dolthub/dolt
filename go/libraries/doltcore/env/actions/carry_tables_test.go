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
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
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

// newEmptyRoot returns a context and the empty working root of a freshly initialized repo.
func newEmptyRoot(t *testing.T) (context.Context, doltdb.RootValue) {
	t.Helper()
	ctx := context.Background()
	emptyRoot, err := dtestutils.CreateTestEnv().WorkingRoot(ctx)
	require.NoError(t, err)
	return ctx, emptyRoot
}

// singleColPKSchema builds a one-column primary key schema using |colName| and |tag|.
func singleColPKSchema(t *testing.T, colName string, tag uint64) schema.Schema {
	t.Helper()
	col, err := schema.NewColumnWithTypeInfo(colName, tag, typeinfo.StringDefaultType, true, "", false, "", schema.NotNullConstraint{})
	require.NoError(t, err)
	return dtestutils.CreateSchema(col)
}

// TestCarryTablesAbsentFromBaseline verifies that CarryTablesAbsentFromBaseline carries untracked tables into the
// target root, resolves tag collisions, and propagates foreign keys.
func TestCarryTablesAbsentFromBaseline(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	const collidingTag uint64 = 9815
	const postsTitleTag uint64 = 13593

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPKSchema(t, "code", collidingTag))
	require.NoError(t, err)

	// Source has bar (so the foreign key can reference it), plus untracked users and posts.
	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, singleColPKSchema(t, "code", collidingTag))
	require.NoError(t, err)
	// bar.code and users.name share collidingTag so the carry must retag users.name.
	usersSch := singleColPKSchema(t, "name", collidingTag)
	_, err = usersSch.Indexes().AddIndexByColTags("idx_name", []uint64{collidingTag}, nil, schema.IndexProperties{IsUserDefined: true, IsUnique: true})
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "users"}, usersSch)
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "posts"}, singleColPKSchema(t, "title", postsTitleTag))
	require.NoError(t, err)

	working = putFK(t, ctx, working, doltdb.ForeignKey{
		Name:                   "fk_bar",
		TableName:              doltdb.TableName{Name: "users"},
		TableColumns:           []uint64{collidingTag},
		ReferencedTableName:    doltdb.TableName{Name: "bar"},
		ReferencedTableColumns: []uint64{collidingTag},
	})

	result, err := CarryTablesAbsentFromBaseline(ctx, working, target, target, nil)
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
		require.NotEqual(t, collidingTag, col.Tag, "users.name must be retagged to avoid colliding with bar.code")

		idx, ok := sch.Indexes().GetByNameCaseInsensitive("idx_name")
		require.True(t, ok, "idx_name must survive the retag")
		require.Equal(t, []uint64{col.Tag}, idx.IndexedColumnTags(), "idx_name must reference the retagged column, not the old tag")
		require.Equal(t, schema.IndexProperties{IsUserDefined: true, IsUnique: true}, idx.Properties(),
			"idx_name properties must survive the retag unchanged")
	})

	t.Run("FK child column retagged, parent column unchanged", func(t *testing.T) {
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
		require.Equal(t, []uint64{collidingTag}, got.ReferencedTableColumns,
			"committed parent column tag must not change")
	})
}

// TestCarryTablesAbsentFromBaselineMergedForeignKeyFollowsRetag verifies that when a carried child's column
// is retagged to resolve a collision, a foreign key already merged onto the target is rewritten to
// the new tag instead of being left pointing at the stale source tag.
func TestCarryTablesAbsentFromBaselineMergedForeignKeyFollowsRetag(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	// Shared by target anchor.x and source child.cid, forcing a retag
	const collide uint64 = 70001
	const parentTag uint64 = 70002

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "id", parentTag))
	require.NoError(t, err)
	target, err = doltdb.CreateEmptyTable(ctx, target, doltdb.TableName{Name: "anchor"}, singleColPKSchema(t, "x", collide))
	require.NoError(t, err)

	working, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "id", parentTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "child"}, singleColPKSchema(t, "cid", collide))
	require.NoError(t, err)

	fk := doltdb.ForeignKey{
		Name:                   "fk_child",
		TableName:              doltdb.TableName{Name: "child"},
		TableColumns:           []uint64{collide},
		ReferencedTableName:    doltdb.TableName{Name: "parent"},
		ReferencedTableColumns: []uint64{parentTag},
	}
	working = putFK(t, ctx, working, fk)
	target = putFK(t, ctx, target, fk)

	result, err := CarryTablesAbsentFromBaseline(ctx, working, target, target, nil)
	require.NoError(t, err)

	childTbl, ok, err := result.GetTable(ctx, doltdb.TableName{Name: "child"})
	require.NoError(t, err)
	require.True(t, ok)
	childSch, err := childTbl.GetSchema(ctx)
	require.NoError(t, err)
	cid, ok := childSch.GetAllCols().GetByName("cid")
	require.True(t, ok)
	require.NotEqual(t, collide, cid.Tag, "child.cid must be retagged off the colliding tag")

	resultFks, err := result.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	got, ok := resultFks.GetByNameCaseInsensitive("fk_child", doltdb.TableName{Name: "child"})
	require.True(t, ok, "fk_child must be present")
	require.Equal(t, []uint64{cid.Tag}, got.TableColumns,
		"carried foreign key must follow the child column retag, not keep the stale source tag")
}

// TestCarryTablesAbsentFromBaselineParentTagMismatch verifies that a foreign key carried from source onto
// target gets its referenced column tags rewritten to match the target parent schema when the
// same parent column has a different tag on source and target.
func TestCarryTablesAbsentFromBaselineParentTagMismatch(t *testing.T) {
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

	result, err := CarryTablesAbsentFromBaseline(ctx, working, target, target, nil)
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

// TestCarryTablesAbsentFromBaselineParentColumnRenamedKeepsSourceTag documents the accepted divergence when
// the referenced parent column has a different name on source and target (renamed on the target
// branch). RemapTagsByColumnName cannot match the source column name against the target parent, so it
// leaves the carried foreign key on the source tag.
func TestCarryTablesAbsentFromBaselineParentColumnRenamedKeepsSourceTag(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11007
	ctx, emptyRoot := newEmptyRoot(t)

	const parentTagOnSource uint64 = 60101
	const parentTagOnTarget uint64 = 60102
	const childTag uint64 = 60103

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "parent"}, singleColPKSchema(t, "ident", parentTagOnTarget))
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

	result, err := CarryTablesAbsentFromBaseline(ctx, working, target, target, nil)
	require.NoError(t, err)

	resultFks, err := result.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	got, ok := resultFks.GetByNameCaseInsensitive("fk_cross", doltdb.TableName{Name: "child"})
	require.True(t, ok, "fk_cross must be present in result FK collection")
	require.Equal(t, []uint64{parentTagOnSource}, got.ReferencedTableColumns,
		"referenced tag stays on the source tag because the parent column name does not exist on target")
	require.Equal(t, []uint64{childTag}, got.TableColumns,
		"child column tag must be unchanged because there was no collision")
}
