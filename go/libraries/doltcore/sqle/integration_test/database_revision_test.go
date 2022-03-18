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

package integration_test

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

type DbRevisionTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.Row
}

func TestDbRevision(t *testing.T) {
	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table myTable (pk int primary key, c0 int);`}},
		{cmd.AddCmd{}, args{"."}},
		{cmd.CommitCmd{}, args{"-m", "added myTable"}},
		{cmd.SqlCmd{}, args{"-q", `insert into myTable values (1,1),(2,2);`}},
		{cmd.CommitCmd{}, args{"-am", "added some data to myTable"}},
	}

	tests := []DbRevisionTest{
		{
			name:  "smoke test",
			setup: []testCommand{},
			query: "select * from myTable",
			rows:  []sql.Row{
				{int32(1), int32(1)},
				{int32(2), int32(2)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testDbRevision(t, test, setupCommon)
		})
	}
}

func testDbRevision(t *testing.T, test DbRevisionTest, setupCommon []testCommand) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	setup := append(setupCommon, test.setup...)
	for _, c := range setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := sqle.ExecuteSelect(t, dEnv, dEnv.DoltDB, root, test.query)
	require.NoError(t, err)

	require.Equal(t, len(test.rows), len(actRows))
	for i := range test.rows {
		assert.Equal(t, len(test.rows[i]), len(actRows[i]))
		for j := range test.rows[i] {
			exp, act := test.rows[i][j], actRows[i][j]

			// special logic for comparing JSONValues
			if js, ok := exp.(json.NomsJSON); ok {
				cmp, err := js.Compare(sql.NewEmptyContext(), act.(json.NomsJSON))
				require.NoError(t, err)
				assert.Zero(t, cmp)
			} else {
				assert.Equal(t, exp, act)
			}
		}
	}
}
