// Copyright 2020 Dolthub, Inc.
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

package schema_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func TestSqlIntegration(t *testing.T) {
	const tblName = "test"
	tests := []struct {
		name      string
		setup     []string
		isKeyless bool
	}{
		{
			name:      "primary key",
			setup:     []string{"CREATE TABLE test (pk int PRIMARY KEY);"},
			isKeyless: false,
		},
		{
			name:      "keyless",
			setup:     []string{"CREATE TABLE test (pk int);"},
			isKeyless: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ddb, root := runTestSql(t, ctx, test.setup)
			defer ddb.Close()

			tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
			require.NoError(t, err)
			require.True(t, ok)
			sch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)

			ok = schema.IsKeyless(sch)
			assert.Equal(t, test.isKeyless, ok)
		})
	}
}

func TestSchemaOrdering(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		pkCols    []string
		otherCols []string
		allCols   []string
		ordinals  []int
	}{
		{
			name:      "primary key",
			query:     "CREATE TABLE t (a int, b int, pk2 int, c int, pk1 int, PRIMARY KEY (pk1, pk2));",
			pkCols:    []string{"pk1", "pk2"},
			otherCols: []string{"a", "b", "c"},
			allCols:   []string{"a", "b", "pk2", "c", "pk1"},
			ordinals:  []int{4, 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ddb, root := runTestSql(t, ctx, []string{test.query})
			defer ddb.Close()

			tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: "t"})
			require.NoError(t, err)
			require.True(t, ok)
			sch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)

			for i, col := range sch.GetPKCols().GetColumns() {
				assert.Equal(t, test.pkCols[i], col.Name)
			}
			for i, col := range sch.GetNonPKCols().GetColumns() {
				assert.Equal(t, test.otherCols[i], col.Name)
			}
			for i, col := range sch.GetAllCols().GetColumns() {
				assert.Equal(t, test.allCols[i], col.Name)
			}
			for i, ord := range sch.GetPkOrdinals() {
				assert.Equal(t, test.ordinals[i], ord)
			}
		})
	}
}

func TestGetKeyTags(t *testing.T) {
	const tblName = "test"
	tests := []struct {
		name    string
		setup   []string
		keyCols []string
	}{
		{
			name:    "primary key",
			setup:   []string{"CREATE TABLE test (pk int PRIMARY KEY, c0 int);"},
			keyCols: []string{"pk"},
		},
		{
			name:    "keyless",
			setup:   []string{"CREATE TABLE test (c0 int, c1 int);"},
			keyCols: nil,
		},
		{
			name:    "secondary index",
			setup:   []string{"CREATE TABLE test (pk int PRIMARY KEY, c0 int, c1 int, INDEX(c0));"},
			keyCols: []string{"pk", "c0"},
		},
		{
			name:    "compound index",
			setup:   []string{"CREATE TABLE test (pk int PRIMARY KEY, c0 int, c1 int, INDEX(c1, c0));"},
			keyCols: []string{"pk", "c0", "c1"},
		},
		{
			name:    "keyless secondary index",
			setup:   []string{"CREATE TABLE test (c0 int, c1 int, INDEX(c1));"},
			keyCols: []string{"c1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ddb, root := runTestSql(t, ctx, test.setup)
			defer ddb.Close()

			tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
			require.NoError(t, err)
			require.True(t, ok)
			sch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)

			all := sch.GetAllCols()
			expected := make([]uint64, len(test.keyCols))
			for i, name := range test.keyCols {
				expected[i] = all.LowerNameToCol[name].Tag
			}
			sort.Slice(expected, func(i, j int) bool {
				return expected[i] < expected[j]
			})

			actual := schema.GetKeyColumnTags(sch)
			assert.Equal(t, expected, actual.AsSlice())
		})
	}
}

func runTestSql(t *testing.T, ctx context.Context, setup []string) (*doltdb.DoltDB, doltdb.RootValue) {
	dEnv := dtestutils.CreateTestEnv()
	cmd := commands.SqlCmd{}
	cliCtx, verr := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, query := range setup {
		code := cmd.Exec(ctx, cmd.Name(), []string{"-q", query}, dEnv, cliCtx)
		require.Equal(t, 0, code)
	}
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	return dEnv.DoltDB, root
}
