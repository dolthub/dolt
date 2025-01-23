// Copyright 2022 Dolthub, Inc.
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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

type DbRevisionTest struct {
	name     string
	setup    []testCommand
	asserts  []testAssert
	asserts2 []dynamicAssert
}

type testAssert struct {
	query string
	rows  []sql.Row
}

type dynamicAssert struct {
	query func() string
	rows  []sql.Row
}

func TestDbRevision(t *testing.T) {
	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table myTable (pk int primary key, c0 int);`}},
		{cmd.AddCmd{}, args{"."}},
		{cmd.CommitCmd{}, args{"-m", "COMMIT(1): added myTable"}},
		{cmd.SqlCmd{}, args{"-q", `insert into myTable values (1,1);`}},
		{cmd.CommitCmd{}, args{"-am", "COMMIT(2): added some data to myTable"}},
		{cmd.BranchCmd{}, args{"other"}},
		{cmd.SqlCmd{}, args{"-q", `insert into myTable values (9,9);`}},
		{cmd.CommitCmd{}, args{"-am", "COMMIT(3): inserted to myTable on branch main"}},
	}

	// each test run must populate these
	var cm1, cm2, cm3 hash.Hash

	tests := []DbRevisionTest{
		{
			name:  "smoke test",
			setup: []testCommand{},
			asserts: []testAssert{
				{
					query: "show databases",
					rows: []sql.Row{
						{"dolt"},
						{"information_schema"},
					},
				},
				{
					query: "select * from myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(9), int32(9)},
					},
				},
			},
		},
		{
			name: "select from branch revision database",
			setup: []testCommand{

				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", `insert into myTable values (19,19);`}},
				{cmd.CommitCmd{}, args{"-am", "COMMIT(4): inserted to myTable on branch other"}},
			},
			asserts: []testAssert{
				{
					query: "select * from dolt.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(19), int32(19)},
					},
				},
				{
					query: "select * from `dolt/other`.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(19), int32(19)},
					},
				},
				{
					query: "select * from `dolt/main`.myTable",
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(9), int32(9)},
					},
				},
			},
			asserts2: []dynamicAssert{
				{
					// lazily construct query string after |cm3| is populated
					query: func() string {
						return fmt.Sprintf("select * from `dolt/%s`.myTable", cm3.String())
					},
					rows: []sql.Row{
						{int32(1), int32(1)},
						{int32(19), int32(19)},
					},
				},
				{
					// lazily construct query string after |cm2| is populated
					query: func() string {
						return fmt.Sprintf("select * from `dolt/%s`.myTable", cm2.String())
					},
					rows: []sql.Row{
						{int32(1), int32(1)},
					},
				},
				{
					// lazily construct query string after |cm1| is populated
					query: func() string {
						return fmt.Sprintf("select * from `dolt/%s`.myTable", cm1.String())
					},
					rows: nil,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()

			cliCtx, _ := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)

			setup := append(setupCommon, test.setup...)
			for _, c := range setup {
				exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
				require.Equal(t, 0, exitCode)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			cm1, cm2, cm3 = populateCommitHashes(t, dEnv, root)
			require.NotEqual(t, cm1, hash.Hash{})
			require.NotEqual(t, cm2, hash.Hash{})
			require.NotEqual(t, cm3, hash.Hash{})

			for _, a := range test.asserts {
				t.Run(a.query, func(t *testing.T) {
					makeTestAssertion(t, a, dEnv, root)
				})
			}
			for _, a2 := range test.asserts2 {
				a := testAssert{
					query: a2.query(),
					rows:  a2.rows,
				}
				t.Run(a.query, func(t *testing.T) {
					makeTestAssertion(t, a, dEnv, root)
				})
			}
		})
	}
}

func populateCommitHashes(t *testing.T, dEnv *env.DoltEnv, root doltdb.RootValue) (cm1, cm2, cm3 hash.Hash) {
	q := "SELECT commit_hash FROM dolt_log;"
	rows, err := sqle.ExecuteSelect(ctx, dEnv, root, q)
	require.NoError(t, err)
	assert.Len(t, rows, 4)
	cm3 = hash.Parse(rows[0][0].(string))
	cm2 = hash.Parse(rows[1][0].(string))
	cm1 = hash.Parse(rows[2][0].(string))
	return
}

func makeTestAssertion(t *testing.T, a testAssert, dEnv *env.DoltEnv, root doltdb.RootValue) {
	actRows, err := sqle.ExecuteSelect(ctx, dEnv, root, a.query)
	require.NoError(t, err)
	assert.Equal(t, a.rows, actRows)
}
