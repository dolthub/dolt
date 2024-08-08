// Copyright 2024 Dolthub, Inc.
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

package kvexec

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// TestLookupJoin ensures that we trigger the operator replacement for
// expected query patterns.
func TestCountAgg(t *testing.T) {
	tests := []struct {
		name      string
		setup     []string
		query     string
		doRowexec bool
	}{
		{
			name: "accept simple field ref",
			setup: []string{
				"create table xy (x int primary key, y int, key y_idx(y))",
			},
			query:     "select count(y) from xy",
			doRowexec: true,
		},
		{
			name: "reject filter child",
			setup: []string{
				"create table xy (x int primary key, y int)",
			},
			query:     "select count(1) from xy where y = 1",
			doRowexec: false,
		},
		{
			name: "reject join child",
			setup: []string{
				"create table xy (x int primary key, y int)",
				"create table ab (a int primary key, b int)",
			},
			query:     "select count(1) from xy join ab on x = a",
			doRowexec: false,
		},
		{
			name: "reject multi parameter",
			setup: []string{
				"create table xy (x int primary key, y int, key y_idx(y))",
			},
			query:     "select count(y), x from xy",
			doRowexec: false,
		},
		{
			name: "accept in subquery with non-nil scope",
			setup: []string{
				"create table xy (x int primary key, y int, key y_idx(y))",
			},
			query:     "select  1, (select count(y) from xy) cnt",
			doRowexec: true,
		},
		{
			name: "reject complex count expressi	on",
			setup: []string{
				"create table xy (x int primary key, y int, key y_idx(y))",
			},
			query:     "select count(y+1), x from xy",
			doRowexec: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB.Close()

			tmpDir, err := dEnv.TempTableFilesDir()
			require.NoError(t, err)

			opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
			db, err := sqle.NewDatabase(context.Background(), "dolt", dEnv.DbData(), opts)
			require.NoError(t, err)

			engine, ctx, err := sqle.NewTestEngine(dEnv, context.Background(), db)
			require.NoError(t, err)

			err = ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
			require.NoError(t, err)

			for _, q := range tt.setup {
				_, iter, _, err := engine.Query(ctx, q)
				require.NoError(t, err)
				_, err = sql.RowIterToRows(ctx, iter)
				require.NoError(t, err)
			}

			binder := planbuilder.New(ctx, engine.EngineAnalyzer().Catalog, engine.Parser)
			node, _, _, qFlags, err := binder.Parse(tt.query, false)
			require.NoError(t, err)
			node, err = engine.EngineAnalyzer().Analyze(ctx, node, nil, qFlags)
			require.NoError(t, err)

			j := getAgg(node)
			require.NotNil(t, j)

			iter, err := Builder{}.Build(ctx, j, nil)
			_, ok := iter.(*countAggKvIter)
			require.Equalf(t, tt.doRowexec, ok, "expected do row exec: %t", tt.doRowexec)
		})
	}
}

func getAgg(n sql.Node) sql.Node {
	var ret sql.Node
	transform.NodeWithOpaque(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if agg, ok := n.(*plan.GroupBy); ok {
			ret = agg
		}
		return n, transform.SameTree, nil
	})
	if ret == nil {
		transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if sq, ok := e.(*plan.Subquery); ok {
				ret = getAgg(sq.Query)
			}
			return e, transform.SameTree, nil
		})
	}
	return ret
}
