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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
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
			dEnv := dtestutils.CreateTestEnv()
			cmd := commands.SqlCmd{}

			for _, query := range test.setup {
				code := cmd.Exec(ctx, cmd.Name(), []string{"-q", query}, dEnv)
				require.Equal(t, 0, code)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, ok, err := root.GetTable(ctx, tblName)
			require.NoError(t, err)
			require.True(t, ok)
			sch, err := tbl.GetSchema(ctx)
			require.NoError(t, err)

			ok = schema.IsKeyless(sch)
			assert.Equal(t, test.isKeyless, ok)
		})
	}
}
