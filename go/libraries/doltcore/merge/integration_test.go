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

package merge_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestMerge(t *testing.T) {

	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY, c0 int);"}},
		{cmd.CommitCmd{}, args{"-am", "created table test"}},
	}

	tests := []struct {
		name  string
		setup []testCommand

		query    string
		expected []sql.Row
	}{
		{
			name:  "smoke test",
			query: "SELECT * FROM test;",
		},
		{
			name: "fast-forward merge",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{"master"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM test",
			expected: []sql.Row{
				{int32(1), int32(1)},
				{int32(2), int32(2)},
			},
		},
		{
			name: "three-way merge",
			setup: []testCommand{
				{cmd.BranchCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (11,11),(22,22);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on master"}},
				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{"master"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM test",
			expected: []sql.Row{
				{int32(1), int32(1)},
				{int32(2), int32(2)},
				{int32(11), int32(11)},
				{int32(22), int32(22)},
			},
		},
		{
			name: "create the same table schema, with different row data, on two branches",
			setup: []testCommand{
				{cmd.BranchCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk varchar(120) primary key);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES ('a'),('b'),('c');"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on master"}},
				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk varchar(120) primary key);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES ('x'),('y'),('z');"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{"master"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM quiz ORDER BY pk",
			expected: []sql.Row{
				{"a"},
				{"b"},
				{"c"},
				{"x"},
				{"y"},
				{"z"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtu.CreateTestEnv()

			for _, tc := range setupCommon {
				tc.exec(t, ctx, dEnv)
			}
			for _, tc := range test.setup {
				tc.exec(t, ctx, dEnv)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			actRows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, test.query)
			require.NoError(t, err)

			require.Equal(t, len(test.expected), len(actRows))
			for i := range test.expected {
				assert.Equal(t, test.expected[i], actRows[i])
			}
		})
	}
}
