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

package doltdb_test

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

type stage struct {
	commands     []testCommand
	preStageFunc func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{}) interface{}
}

type gcTest struct {
	name       string
	stages     []stage
	query      string
	expected   []sql.Row
	postGCFunc func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{})
}

var gcTests = []gcTest{
	{
		name: "gc test",
		stages: []stage{
			{
				preStageFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, i interface{}) interface{} {
					return nil
				},
				commands: []testCommand{
					{commands.CheckoutCmd{}, []string{"-b", "temp"}},
					{commands.SqlCmd{}, []string{"-q", "INSERT INTO test VALUES (0),(1),(2);"}},
					{commands.AddCmd{}, []string{"."}},
					{commands.CommitCmd{}, []string{"-m", "commit"}},
				},
			},
			{
				preStageFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, i interface{}) interface{} {
					cm, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("temp"))
					require.NoError(t, err)
					h, err := cm.HashOf()
					require.NoError(t, err)
					cs, err := doltdb.NewCommitSpec(h.String())
					require.NoError(t, err)
					_, err = ddb.Resolve(ctx, cs, nil)
					require.NoError(t, err)
					return h
				},
				commands: []testCommand{
					{commands.CheckoutCmd{}, []string{env.DefaultInitBranch}},
					{commands.BranchCmd{}, []string{"-D", "temp"}},
					{commands.SqlCmd{}, []string{"-q", "INSERT INTO test VALUES (4),(5),(6);"}},
				},
			},
		},
		query:    "select * from test;",
		expected: []sql.Row{{int32(4)}, {int32(5)}, {int32(6)}},
		postGCFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{}) {
			h := prevRes.(hash.Hash)
			cs, err := doltdb.NewCommitSpec(h.String())
			require.NoError(t, err)
			_, err = ddb.Resolve(ctx, cs, nil)
			require.Error(t, err)
		},
	},
}

var gcSetupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "CREATE TABLE test (pk int PRIMARY KEY)"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "created test table"}},
}

func TestGarbageCollection(t *testing.T) {
	require.True(t, true)
	assert.True(t, true)

	restoreIO := cli.InitIO()
	defer restoreIO()

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

	var res interface{}
	for _, stage := range test.stages {
		res = stage.preStageFunc(ctx, t, dEnv.DoltDB, res)
		for _, c := range stage.commands {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
			require.Equal(t, 0, exitCode)
		}
	}

	working, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	h, err := working.HashOf()
	require.NoError(t, err)
	// save working root during GC

	err = dEnv.DoltDB.GC(ctx, h)
	require.NoError(t, err)
	test.postGCFunc(ctx, t, dEnv.DoltDB, res)

	working, err = dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	// assert all out rows are present after gc
	actual, err := sqle.ExecuteSelect(t, dEnv, dEnv.DoltDB, working, test.query)
	require.NoError(t, err)
	assert.Equal(t, test.expected, actual)
}
