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

	tests := []jsonValueTest{
		{
			name: "create JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `create table js (pk int primary key, js json);`}},
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '[]'), (2, '{"a":1}');`}},
			},
			query: "select count(*) from js",
			rows:  []sql.Row{{int64(2)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()

			for _, c := range test.setup {
				exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
				require.Equal(t, 0, exitCode)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			actRows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, test.query)
			require.NoError(t, err)

			require.Equal(t, len(test.rows), len(actRows))
			for i := range test.rows {
				assert.Equal(t, test.rows[i], actRows[i])
			}
		})
	}
}
