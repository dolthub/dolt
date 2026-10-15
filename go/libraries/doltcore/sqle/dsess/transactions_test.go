// Copyright 2021 Dolthub, Inc.
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

package dsess

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

func TestPrepareDoltCommitReturnsHeadUsedForPreparation(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	t.Cleanup(func() { ddb.Close() })
	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", "Test User", "test@example.com"))
	headRef := ref.NewBranchRef("main")
	spec, err := doltdb.NewCommitSpec("HEAD")
	require.NoError(t, err)
	optional, err := ddb.Resolve(ctx, spec, headRef)
	require.NoError(t, err)
	initial, ok := optional.ToCommit()
	require.True(t, ok)
	root, err := initial.GetRootValue(ctx)
	require.NoError(t, err)
	ws := doltdb.EmptyWorkingSet(ref.NewWorkingSetRef("heads/main")).WithWorkingRoot(root).WithStagedRoot(root)
	pending := &doltdb.PendingCommit{
		Roots:         doltdb.Roots{Head: root, Working: root, Staged: root},
		CommitOptions: datas.CommitOptions{Meta: &datas.CommitMeta{Description: "pending"}},
	}

	// Advance HEAD without changing its root. Preparation must return commit identity,
	// rather than the transaction's original HEAD or the root being committed.
	advance := doltdb.BranchHeadUpdate{
		HeadRef:       headRef,
		Root:          root,
		CommitOptions: datas.CommitOptions{Meta: &datas.CommitMeta{Description: "concurrent"}},
	}
	heads, err := ddb.CommitHeadUpdates(ctx, []doltdb.HeadUpdate{advance}, nil)
	require.NoError(t, err)
	_, prepared, expected, err := prepareDoltCommit(ctx, "mydb", ddb, ws, pending, ws, editor.Options{})
	require.NoError(t, err)
	require.Equal(t, heads[0], expected)

	// A subsequent head-only write must invalidate the prepared commit.
	_, err = ddb.CommitHeadUpdates(ctx, []doltdb.HeadUpdate{advance}, nil)
	require.NoError(t, err)
	before, err := ddb.NomsRoot(ctx)
	require.NoError(t, err)
	_, err = ddb.CommitHeadUpdates(ctx, []doltdb.HeadUpdate{doltdb.BranchHeadUpdate{
		HeadRef:       headRef,
		Root:          prepared.Roots.Staged,
		CommitOptions: prepared.CommitOptions,
		ExpectedHead:  expected,
	}}, nil)
	require.ErrorIs(t, err, datas.ErrMergeNeeded)
	after, err := ddb.NomsRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
