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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
)

type DbRevisionTest struct {
	name    string
	setup   []testCommand
	asserts []testAssert
}

type testAssert struct {
	query string
	rows  []sql.Row
}

func TestDbRevision(t *testing.T) {
	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table myTable (pk int primary key, c0 int);`}},
		{cmd.AddCmd{}, args{"."}},
		{cmd.CommitCmd{}, args{"-m", "COMMIT(1): added myTable"}},
		{cmd.SqlCmd{}, args{"-q", `insert into myTable values (1,1),(2,2);`}},
		{cmd.CommitCmd{}, args{"-am", "COMMIT(2): added some data to myTable"}},
	}

	tests := []DbRevisionTest{
		{
			name:  "smoke test",
			setup: []testCommand{},
			asserts: []testAssert{
				{
					query: "show databases",
					rows: []sql.Row{
						{"dolt"},
					},
				},
				{
					query: "select * from myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(2), int32(2)},
					},
				},
			},
		},
		{
			name: "select from branch revision database",
			setup: []testCommand{
				{cmd.BranchCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", `insert into myTable values (8,8);`}},
				{cmd.CommitCmd{}, args{"-am", "COMMIT(3a): inserted to myTable on branch main"}},
				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", `insert into myTable values (18,18);`}},
				{cmd.CommitCmd{}, args{"-am", "COMMIT(3b): inserted to myTable on branch other"}},
			},
			asserts: []testAssert{
				{
					query: "select * from dolt.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(18), int32(18)},
					},
				},
				{
					query: "select * from dolt.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(18), int32(18)},
					},
				},
				{
					query: "select * from `dolt/other`.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(18), int32(18)},
					},
				},
				{
					query: "select * from `dolt/main`.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(2), int32(2)},
						{int32(8), int32(8)},
					},
				},
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
	for _, a := range test.asserts {
		makeTestAssertion(t, a, dEnv, root)
	}
}

func makeTestAssertion(t *testing.T, a testAssert, dEnv *env.DoltEnv, root *doltdb.RootValue) {
	actRows, err := sqle.ExecuteSelect(t, dEnv, dEnv.DoltDB, root, a.query)
	require.NoError(t, err)

	require.Equal(t, len(a.rows), len(actRows))
	for i := range a.rows {
		assert.Equal(t, len(a.rows[i]), len(actRows[i]))
		for j := range a.rows[i] {
			exp, act := a.rows[i][j], actRows[i][j]

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
