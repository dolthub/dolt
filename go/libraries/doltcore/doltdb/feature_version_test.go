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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

const (
	newVersion doltdb.FeatureVersion = 1 << 19
	oldVersion doltdb.FeatureVersion = 1 << 13
)

// |doltdb.FeatureVersion| is manipulated during these integration tests.
// Save a copy here to assert that it was correctly restored.
var DoltFeatureVersionCopy = doltdb.DoltFeatureVersion

type fvTest struct {
	name   string
	setup  []fvCommand
	expVer doltdb.FeatureVersion

	// for error path testing
	errCmds []fvCommand
}

type args []string

type fvCommand struct {
	user fvUser
	cmd  cli.Command
	args args
}

func (cmd fvCommand) exec(ctx context.Context, dEnv *env.DoltEnv) int {
	// execute the command using |cmd.user|'s Feature Version
	doltdb.DoltFeatureVersion = cmd.user.vers
	defer func() { doltdb.DoltFeatureVersion = DoltFeatureVersionCopy }()

	cliCtx, _ := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)

	return cmd.cmd.Exec(ctx, cmd.cmd.Name(), cmd.args, dEnv, cliCtx)
}

type fvUser struct {
	vers doltdb.FeatureVersion
}

var NewClient = fvUser{vers: newVersion}
var OldClient = fvUser{vers: oldVersion}

func TestFeatureVersion(t *testing.T) {

	tests := []fvTest{
		{
			name: "smoke test",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
			},
			expVer: oldVersion,
		},
		{
			name: "CREATE TABLE statements write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk int PRIMARY KEY);"}},
			},
			expVer: newVersion,
		},
		{
			name: "DROP TABLE statements write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "DROP TABLE test;"}},
			},
			expVer: newVersion,
		},
		{
			name: "schema changes write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "ALTER TABLE test ADD COLUMN c0 int;"}},
			},
			expVer: newVersion,
		},
		{
			name: "INSERT statements write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (0);"}},
			},
			expVer: newVersion,
		},
		{
			name: "UPDATE statements write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{OldClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (0);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "UPDATE test SET pk = 1;"}},
			},
			expVer: newVersion,
		},
		{
			name: "DELETE statements write feature version",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{OldClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (0);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "DELETE FROM test WHERE pk = 0;"}},
			},
			expVer: newVersion,
		},
		{
			name: "new client writes to table, locking out old client",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{OldClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (0);"}},
				{NewClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1);"}},
			},
			errCmds: []fvCommand{
				// old client can't write
				{OldClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (2);"}},
				// old client can't read
				{OldClient, commands.SqlCmd{}, args{"-q", "SELECT * FROM test;"}},
			},
			expVer: newVersion,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			doltdb.DoltFeatureVersion = oldVersion
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB.Close()
			doltdb.DoltFeatureVersion = DoltFeatureVersionCopy

			for _, cmd := range test.setup {
				code := cmd.exec(ctx, dEnv)
				require.Equal(t, 0, code)
			}
			for _, cmd := range test.errCmds {
				code := cmd.exec(ctx, dEnv)
				require.NotEqual(t, 0, code)
			}

			// execute assertions with newVersion to avoid OutOfDate errors
			doltdb.DoltFeatureVersion = newVersion
			defer func() { doltdb.DoltFeatureVersion = DoltFeatureVersionCopy }()

			assertFeatureVersion := func(r doltdb.RootValue) {
				act, ok, err := r.GetFeatureVersion(ctx)
				require.NoError(t, err)
				require.True(t, ok)
				assert.Equal(t, test.expVer, act)
			}

			working, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			assertFeatureVersion(working)
		})

		// ensure |doltdb.DoltFeatureVersion| was restored
		assert.Equal(t, DoltFeatureVersionCopy, doltdb.DoltFeatureVersion)
	}
}
