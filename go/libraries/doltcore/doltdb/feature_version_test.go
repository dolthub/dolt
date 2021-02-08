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
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/stretchr/testify/require"
)

const (
	newVersion doltdb.FeatureVersion = 19
	oldVersion doltdb.FeatureVersion = 13
)

type fvUser struct {
	vers doltdb.FeatureVersion
}

var NewClient = fvUser{vers: newVersion}
var OldClient = fvUser{vers: oldVersion}

type args []string

type fvCommand struct {
	user fvUser
	cmd  cli.Command
	args args
}

func (cmd fvCommand) exec(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) {
	// execute the command using |cmd.user|'s Feature Version
	doltdb.DoltFeatureVersion, cmd.user.vers = cmd.user.vers, doltdb.DoltFeatureVersion
	defer func() { doltdb.DoltFeatureVersion = cmd.user.vers }()

	act := doltdb.DoltFeatureVersion
	fmt.Println(act)
	exitCode := cmd.cmd.Exec(ctx, cmd.cmd.Name(), cmd.args, dEnv)
	require.Equal(t, 0, exitCode)
}

type fvTest struct {
	name     string
	setup    []fvCommand
	expected doltdb.FeatureVersion
}

func TestFeatureVersion(t *testing.T) {

	tests := []fvTest{
		{
			name: "smoke test",
			setup: []fvCommand{
				{OldClient, commands.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY);"}},
				{OldClient, commands.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (0);"}},
			},
			expected: oldVersion,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()

			for _, cmd := range test.setup {
				cmd.exec(t, ctx, dEnv)
			}

			// execute assertions with max feature version
			doltdb.DoltFeatureVersion = math.MaxInt64

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			act, ok, err := root.GetFeatureVersion(ctx)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, test.expected, act)
		})
	}
}
