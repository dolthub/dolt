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

package envtestutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// TableUpdate defines a list of modifications that should be made to a table
type TableUpdate struct {
	// NewSch is an updated schema for this table. It overwrites the existing value.  If not provided the existing value
	// will not change
	NewSch schema.Schema

	// NewRowData if provided overwrites the entirety of the row data in the table.
	NewRowData *types.Map

	// RowUpdates are new values for rows that should be set in the map.  They can be updates or inserts.
	RowUpdates []row.Row
}

// HistoryNode represents a commit to be made
type HistoryNode struct {
	// Branch the branch that the commit should be on
	Branch string

	// CommitMessag is the commit message that should be applied
	CommitMsg string

	// Updates are the changes that should be made to the table's states before committing
	Updates map[string]TableUpdate

	// Children are the child commits of this commit
	Children []HistoryNode
}

// InitializeWithHistory will go through the provided historyNodes and create the intended commit graph
func InitializeWithHistory(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, historyNodes ...HistoryNode) {
	for _, node := range historyNodes {
		cs, err := doltdb.NewCommitSpec("master", "")
		require.NoError(t, err)

		cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
		require.NoError(t, err)

		processNode(t, ctx, dEnv, node, cm)
	}
}

func processNode(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, node HistoryNode, parent *doltdb.Commit) {
	branchRef := ref.NewBranchRef(node.Branch)
	ok, err := dEnv.DoltDB.HasRef(ctx, branchRef)
	require.NoError(t, err)

	if !ok {
		err = dEnv.DoltDB.NewBranchAtCommit(ctx, branchRef, parent)
		require.NoError(t, err)
	}

	cs, err := doltdb.NewCommitSpec(branchRef.String(), "")
	require.NoError(t, err)

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	require.NoError(t, err)

	root, err := cm.GetRootValue()
	require.NoError(t, err)

	root = UpdateTables(t, ctx, root, node.Updates)
	h, err := dEnv.DoltDB.WriteRootValue(ctx, root)
	require.NoError(t, err)

	meta, err := doltdb.NewCommitMeta("Ash Ketchum", "ash@poke.mon", node.CommitMsg)
	require.NoError(t, err)

	cm, err = dEnv.DoltDB.Commit(ctx, h, branchRef, meta)
	require.NoError(t, err)

	for _, child := range node.Children {
		processNode(t, ctx, dEnv, child, cm)
	}
}

func UpdateTables(t *testing.T, ctx context.Context, root *doltdb.RootValue, tblUpdates map[string]TableUpdate) *doltdb.RootValue {
	for tblName, updates := range tblUpdates {
		tbl, ok, err := root.GetTable(ctx, tblName)
		require.NoError(t, err)

		var sch schema.Schema
		if updates.NewSch != nil {
			sch = updates.NewSch
		} else {
			sch, err = tbl.GetSchema(ctx)
			require.NoError(t, err)
		}

		var rowData types.Map
		if updates.NewRowData == nil {
			if ok {
				rowData, err = tbl.GetRowData(ctx)
				require.NoError(t, err)
			} else {
				rowData, err = types.NewMap(ctx, root.VRW())
				require.NoError(t, err)
			}
		} else {
			rowData = *updates.NewRowData
		}

		if updates.RowUpdates != nil {
			me := rowData.Edit()

			for _, r := range updates.RowUpdates {
				me = me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
			}

			rowData, err = me.Map(ctx)
			require.NoError(t, err)
		}

		schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, root.VRW(), sch)

		var indexData *types.Map
		if tbl != nil {
			existingIndexData, err := tbl.GetIndexData(ctx)
			require.NoError(t, err)
			indexData = &existingIndexData
		}
		tbl, err = doltdb.NewTable(ctx, root.VRW(), schVal, rowData, indexData)
		require.NoError(t, err)

		root, err = root.PutTable(ctx, tblName, tbl)
		require.NoError(t, err)
	}

	return root
}
