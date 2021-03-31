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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
)

type jsonValueTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.Row
}

func TestJsonValues(t *testing.T) {
	json.FeatureFlag = true
	defer func() { json.FeatureFlag = false }()

	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table js (pk int primary key, js json);`}},
	}

	tests := []jsonValueTest{
		{
			name: "create JSON table",
			setup: []testCommand{},
			query: "select * from js",
			rows:  []sql.Row{},
		},
		{
			name: "insert into a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
			},
			query: "select * from js",
			rows:  []sql.Row{
				{int32(1), json.MustNomsJSON(`{"a":1}`)},
				{int32(2), json.MustNomsJSON(`{"b":2}`)},
			},
		},
		{
			name: "update a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
				{cmd.SqlCmd{}, args{"-q", `update js set js = '{"c":3}' where pk = 2;`}},
			},
			query: "select * from js",
			rows:  []sql.Row{
				{int32(1), json.MustNomsJSON(`{"a":1}`)},
				{int32(2), json.MustNomsJSON(`{"c":3}`)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()

			setup := append(setupCommon, test.setup...)
			for _, c := range setup {
				exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
				require.Equal(t, 0, exitCode)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			actRows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, test.query)
			require.NoError(t, err)

			require.Equal(t, len(test.rows), len(actRows))
			for i := range test.rows {
				assert.Equal(t, len(test.rows[i]), len(actRows[i]))
				for j := range test.rows[i] {
					exp, act := test.rows[i][j], actRows[i][j]

					// special logic JSONValues
					if js, ok := exp.(json.NomsJSON); ok {
						cmp, err := js.Compare(sql.NewEmptyContext(), act.(json.NomsJSON))
						require.NoError(t, err)
						assert.Zero(t, cmp)
					} else {
						assert.Equal(t, exp, act)
					}
				}
			}
		})
	}
}
