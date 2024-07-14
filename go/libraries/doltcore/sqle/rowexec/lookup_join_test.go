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

package rowexec

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestLookupJoin ensures that we trigger the operator replacement for
// expected query patterns.
func TestLookupJoin(t *testing.T) {
	tests := []struct {
		name      string
		setup     []string
		join      string
		doRowexec bool
	}{
		{
			name: "simple lookup join converts",
			setup: []string{
				"create table xy (x int primary key, y int)",
				"create table ab (a int primary key, b int)",
			},
			join:      "select  /*+ LOOKUP_JOIN(xy,ab) */ * from xy join ab on x = a",
			doRowexec: true,
		},
		{
			name: "type incompatibility doesn't convert",
			setup: []string{
				"create table xy (x int primary key, y int)",
				"create table ab (a varchar(10) primary key, b int)",
			},
			join:      "select  /*+ LOOKUP_JOIN(xy,ab) */ * from xy join ab on x = a",
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
				_, iter, err := engine.Query(ctx, q)
				require.NoError(t, err)
				_, err = sql.RowIterToRows(ctx, iter)
				require.NoError(t, err)
			}

			binder := planbuilder.New(ctx, engine.EngineAnalyzer().Catalog, engine.Parser)
			node, _, _, err := binder.Parse(tt.join, false)
			require.NoError(t, err)
			node, err = engine.EngineAnalyzer().Analyze(ctx, node, nil)
			require.NoError(t, err)

			var j sql.Node
			transform.InspectUp(node, func(node sql.Node) bool {
				if join, ok := node.(*plan.JoinNode); ok {
					j = join
				}
				return false
			})
			iter, err := Builder{}.Build(ctx, j, nil)
			_, ok := iter.(*lookupJoinKvIter)
			require.Equalf(t, tt.doRowexec, ok, "expected do row exec: %t", tt.doRowexec)
		})
	}
}
