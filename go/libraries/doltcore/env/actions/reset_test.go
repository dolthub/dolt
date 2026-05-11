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

// TestMoveUntrackedTables verifies that MoveUntrackedTables correctly copies untracked tables
// into the target root, retagging any column whose tag collides with an existing tag in the target.
// See https://github.com/dolthub/dolt/issues/11007
func TestMoveUntrackedTables(t *testing.T) {
	dEnv, _ := createTestEnv()
	ctx := context.Background()
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main"))

	emptyRoot, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	const barCodeTag uint64 = 9815
	const usersNameTag uint64 = 9815  // deliberate collision with barCodeTag
	const postsTitleTag uint64 = 13593

	mkSch := func(colName string, tag uint64) schema.Schema {
		col := schema.NewColumn(colName, tag, types.StringKind, true, schema.NotNullConstraint{})
		sch, err := schema.SchemaFromCols(schema.NewColCollection(col))
		require.NoError(t, err)
		require.NoError(t, sch.SetPkOrdinals([]int{0}))
		return sch
	}

	target, err := doltdb.CreateEmptyTable(ctx, emptyRoot, doltdb.TableName{Name: "bar"}, mkSch("code", barCodeTag))
	require.NoError(t, err)

	working := emptyRoot
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "users"}, mkSch("name", usersNameTag))
	require.NoError(t, err)
	working, err = doltdb.CreateEmptyTable(ctx, working, doltdb.TableName{Name: "posts"}, mkSch("title", postsTitleTag))
	require.NoError(t, err)

	result, err := MoveUntrackedTables(ctx, working, emptyRoot, target)
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
	})
}
