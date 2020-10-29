// Copyright 2020 Liquidata, Inc.
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

package doltdb_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/types"
)

type gcTest struct {
	name     string
	setup    []testCommand
	garbage  types.Value
	query    string
	expected []sql.Row
}

var gcTests = []gcTest{
	{
		name: "gc test",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "INSERT INTO test VALUES (0),(1),(2);"}},
		},
		garbage: types.String("supercalifragilisticexpialidocious"),
	},
}

var gcSetupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "CREATE TABLE test (pk int PRIMARY KEY)"}},
}

func TestGarbageCollection(t *testing.T) {
	require.True(t, true)
	assert.True(t, true)

	for _, gct := range gcTests {
		t.Run(gct.name, func(t *testing.T) {
			testGarbageCollection(t, gct)
		})
	}

}

func testGarbageCollection(t *testing.T, test gcTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	for _, c := range gcSetupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}
	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	garbageRef, err := dEnv.DoltDB.ValueReadWriter().WriteValue(ctx, test.garbage)
	require.NoError(t, err)
	val, err := dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, garbageRef.TargetHash())
	require.NoError(t, err)
	assert.NotNil(t, val)

	working, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	h, err := working.HashOf()
	require.NoError(t, err)
	// save working root during GC
	err = dEnv.DoltDB.GC(ctx, h)
	require.NoError(t, err)

	working, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	// assert all out rows are present after gc
	actual, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, working, test.query)
	require.NoError(t, err)
	assert.Equal(t, test.expected, actual)

	// assert that garbage was collected
	val, err = dEnv.DoltDB.ValueReadWriter().ReadValue(ctx, garbageRef.TargetHash())
	require.NoError(t, err)
	assert.Nil(t, val)
}
