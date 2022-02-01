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

package alterschema_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/types"
)

type testCommand struct {
	cmd  cli.Command
	args []string
}

func (tc testCommand) exec(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) {
	exitCode := tc.cmd.Exec(ctx, tc.cmd.Name(), tc.args, dEnv)
	require.Equal(t, 0, exitCode)
}

var setupAdd = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (id int not null, c1 int);"}},
	{commands.SqlCmd{}, []string{"-q", "create index c1_idx on test(c1)"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values (1,1),(2,2)"}},
}

func TestAddPk(t *testing.T) {
	t.Run("Add primary key to table with index", func(t *testing.T) {
		dEnv := dtestutils.CreateTestEnv()
		ctx := context.Background()

		for _, c := range setupAdd {
			c.exec(t, ctx, dEnv)
		}

		table, err := getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		// Get the original index data
		originalMap, err := table.GetNomsIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, originalMap.Empty())

		exitCode := commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD constraint test_check CHECK (c1 > 0)"}, dEnv)
		require.Equal(t, 0, exitCode)

		exitCode = commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD PRIMARY KEY(id)"}, dEnv)
		require.Equal(t, 0, exitCode)

		table, err = getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		sch, err := table.GetSchema(ctx)
		assert.NoError(t, err)

		assert.Equal(t, 1, sch.Checks().Count())
		assert.Equal(t, "test_check", sch.Checks().AllChecks()[0].Name())

		// Assert the new index map is not empty
		newMap, err := table.GetNomsIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))

		// Assert the noms level encoding of the map by ensuring the correct index values are present
		kr1, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(1), types.Int(1)})
		assert.NoError(t, err)

		idx, ok := sch.Indexes().GetByNameCaseInsensitive(indexName)
		assert.True(t, ok)

		full, _, _, err := kr1.ReduceToIndexKeys(idx, nil)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)

		kr2, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(2), types.Int(2)})
		assert.NoError(t, err)

		full, _, _, err = kr2.ReduceToIndexKeys(idx, nil)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("Add primary key with diff column set than before", func(t *testing.T) {
		dEnv := dtestutils.CreateTestEnv()
		ctx := context.Background()

		for _, c := range setupAdd {
			c.exec(t, ctx, dEnv)
		}

		table, err := getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		exitCode := commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD PRIMARY KEY (c1)"}, dEnv)
		require.Equal(t, 0, exitCode)

		table, err = getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		sch, err := table.GetSchema(ctx)
		assert.NoError(t, err)

		// Assert the new index map is not empty
		newMap, err := table.GetNomsRowData(ctx)
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))

		// Assert the noms level encoding of the map by ensuring the correct index values are present
		kr1, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(1), types.Int(1)})
		assert.NoError(t, err)
		kr1Key, err := kr1.NomsMapKey(sch).Value(ctx)
		assert.NoError(t, err)

		ok, err := newMap.Has(ctx, kr1Key)
		assert.NoError(t, err)
		assert.True(t, ok)

		kr2, err := createRow(sch, sch.GetAllCols().Tags, []types.Value{types.Int(2), types.Int(2)})
		assert.NoError(t, err)
		kr2Key, err := kr2.NomsMapKey(sch).Value(ctx)
		assert.NoError(t, err)

		ok, err = newMap.Has(ctx, kr2Key)
		assert.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("Add primary key when one more cells contain NULL", func(t *testing.T) {
		dEnv := dtestutils.CreateTestEnv()
		ctx := context.Background()

		for _, c := range setupAdd {
			c.exec(t, ctx, dEnv)
		}

		_, err := getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		exitCode := commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD PRIMARY KEY (c1)"}, dEnv)
		require.Equal(t, 0, exitCode)

		exitCode = commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD COLUMN (c2 INT NULL)"}, dEnv)
		require.Equal(t, 0, exitCode)

		exitCode = commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test DROP PRIMARY KEY"}, dEnv)
		require.Equal(t, 0, exitCode)

		exitCode = commands.SqlCmd{}.Exec(ctx, "sql", []string{"-q", "ALTER TABLE test ADD PRIMARY KEY (id, c1, c2)"}, dEnv)
		require.Equal(t, 1, exitCode)
	})
}
