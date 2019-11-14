package envtestutils

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"testing"
)

type TableUpdate struct {
	NewSch     schema.Schema
	NewRowData *types.Map
	RowUpdates []row.Row
}

type HistoryNode struct {
	Branch    string
	CommitMsg string
	Children  []HistoryNode
	Updates   map[string]TableUpdate
}

func InitializeWithHistory(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, historyNodes ...HistoryNode) {
	for _, node := range historyNodes {
		cs, err := doltdb.NewCommitSpec("HEAD", "master")
		require.NoError(t, err)

		cm, err := dEnv.DoltDB.Resolve(ctx, cs)
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

	cs, err := doltdb.NewCommitSpec("HEAD", branchRef.String())
	require.NoError(t, err)

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)
	require.NoError(t, err)

	root, err := cm.GetRootValue()
	require.NoError(t, err)

	root = updateTables(t, ctx, root, node.Updates)
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

func updateTables(t *testing.T, ctx context.Context, root *doltdb.RootValue, tblUpdates map[string]TableUpdate) *doltdb.RootValue {
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

		schVal, err := encoding.MarshalAsNomsValue(ctx, root.VRW(), sch)
		tbl, err = doltdb.NewTable(ctx, root.VRW(), schVal, rowData)
		require.NoError(t, err)

		root, err = root.PutTable(ctx, tblName, tbl)
		require.NoError(t, err)
	}

	return root
}
