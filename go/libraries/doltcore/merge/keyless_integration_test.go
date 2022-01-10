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

package merge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const tblName = "noKey"

var sch = dtu.MustSchema(
	schema.NewColumn("c1", 1, types.IntKind, false),
	schema.NewColumn("c2", 2, types.IntKind, false),
)
var c1Tag = types.Uint(1)
var c2Tag = types.Uint(2)
var cardTag = types.Uint(schema.KeylessRowCardinalityTag)

func TestKeylessMerge(t *testing.T) {

	tests := []struct {
		name     string
		setup    []testCommand
		expected tupleSet
	}{
		{
			name: "fast-forward merge",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
			),
		},
		{
			name: "3-way merge",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (5,6);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(5), c2Tag, types.Int(6)),
			),
		},
		{
			name: "3-way merge with duplicates",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4), (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (5,6), (5,6);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(3), c2Tag, types.Int(4)),
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(5), c2Tag, types.Int(6)),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtu.CreateTestEnv()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			root, err = root.CreateEmptyTable(ctx, tblName, sch)
			require.NoError(t, err)
			err = dEnv.UpdateWorkingRoot(ctx, root)
			require.NoError(t, err)

			for _, c := range test.setup {
				exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
				require.Equal(t, 0, exitCode)
			}

			root, err = dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tblName)
			require.NoError(t, err)

			assertKeylessRows(t, ctx, tbl, test.expected)
		})
	}
}

func TestKeylessMergeConflicts(t *testing.T) {
	tests := []struct {
		name  string
		setup []testCommand

		// Tuple(
		//    Tuple(baseVal)
		//    Tuple(val)
		//    Tuple(mergeVal)
		// )
		conflicts tupleSet

		oursExpected   tupleSet
		theirsExpected tupleSet
	}{
		{
			name: "identical parallel changes",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: mustTupleSet(
				dtu.MustTuple(
					types.NullValue,
					dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
					dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
				),
			),
			oursExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
			),
			theirsExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(3), c2Tag, types.Int(4)),
			),
		},
		{
			name: "asymmetric parallel deletes",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2),(1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "delete from noKey where (c1,c2) = (1,2) limit 1;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 1 row on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "delete from noKey where (c1,c2) = (1,2) limit 2;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 2 rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: mustTupleSet(
				dtu.MustTuple(
					dtu.MustTuple(cardTag, types.Uint(4), c1Tag, types.Int(1), c2Tag, types.Int(2)),
					dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
					dtu.MustTuple(cardTag, types.Uint(3), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				),
			),
			oursExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
			),
			theirsExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(3), c1Tag, types.Int(1), c2Tag, types.Int(2)),
			),
		},
		{
			name: "asymmetric parallel updates",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2),(1,2),(1,2);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "update noKey set c2 = 9 limit 1;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 1 row on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "update noKey set c2 = 9 limit 2;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 2 rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: mustTupleSet(
				dtu.MustTuple(
					dtu.MustTuple(cardTag, types.Uint(4), c1Tag, types.Int(1), c2Tag, types.Int(2)),
					dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
					dtu.MustTuple(cardTag, types.Uint(3), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				),
				dtu.MustTuple(
					types.NullValue,
					dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(9)),
					dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(1), c2Tag, types.Int(9)),
				),
			),
			oursExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(2), c1Tag, types.Int(1), c2Tag, types.Int(9)),
			),
			theirsExpected: mustTupleSet(
				dtu.MustTuple(cardTag, types.Uint(3), c1Tag, types.Int(1), c2Tag, types.Int(2)),
				dtu.MustTuple(cardTag, types.Uint(1), c1Tag, types.Int(1), c2Tag, types.Int(9)),
			),
		},
	}

	setupTest := func(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, cc []testCommand) {
		root, err := dEnv.WorkingRoot(ctx)
		require.NoError(t, err)
		root, err = root.CreateEmptyTable(ctx, tblName, sch)
		require.NoError(t, err)
		err = dEnv.UpdateWorkingRoot(ctx, root)
		require.NoError(t, err)

		for _, c := range cc {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
			require.Equal(t, 0, exitCode)
		}
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()
			setupTest(t, ctx, dEnv, test.setup)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tblName)
			require.NoError(t, err)
			_, conflicts, err := tbl.GetConflicts(ctx)
			require.NoError(t, err)

			assert.True(t, conflicts.Len() > 0)
			assert.Equal(t, int(conflicts.Len()), len(test.conflicts))

			actual, err := conflicts.Iterator(ctx)
			require.NoError(t, err)
			for {
				_, act, err := actual.Next(ctx)
				if act == nil {
					return
				}
				assert.NoError(t, err)
				h, err := act.Hash(types.Format_Default)
				assert.NoError(t, err)
				exp, ok := test.conflicts[h]
				assert.True(t, ok)
				assert.True(t, exp.Equals(act))
			}
		})

		// conflict resolution

		t.Run(test.name+"_resolved_ours", func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()

			setupTest(t, ctx, dEnv, test.setup)

			resolve := cnfcmds.ResolveCmd{}
			args := []string{"--ours", tblName}
			exitCode := resolve.Exec(ctx, resolve.Name(), args, dEnv)
			require.Equal(t, 0, exitCode)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tblName)
			require.NoError(t, err)

			assertKeylessRows(t, ctx, tbl, test.oursExpected)
		})
		t.Run(test.name+"_resolved_theirs", func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()

			setupTest(t, ctx, dEnv, test.setup)

			resolve := cnfcmds.ResolveCmd{}
			args := []string{"--theirs", tblName}
			exitCode := resolve.Exec(ctx, resolve.Name(), args, dEnv)
			require.Equal(t, 0, exitCode)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tblName)
			require.NoError(t, err)

			assertKeylessRows(t, ctx, tbl, test.theirsExpected)
		})
	}
}

// |expected| is a tupleSet to compensate for random storage order
func assertKeylessRows(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected tupleSet) {
	rowData, err := tbl.GetNomsRowData(ctx)
	require.NoError(t, err)

	assert.Equal(t, int(rowData.Len()), len(expected))

	actual, err := rowData.Iterator(ctx)
	require.NoError(t, err)
	for {
		_, act, err := actual.Next(ctx)
		if act == nil {
			return
		}
		assert.NoError(t, err)
		h, err := act.Hash(types.Format_Default)
		assert.NoError(t, err)
		exp, ok := expected[h]
		assert.True(t, ok)
		assert.True(t, exp.Equals(act))
	}
}

type tupleSet map[hash.Hash]types.Tuple

func mustTupleSet(tt ...types.Tuple) (s tupleSet) {
	s = make(tupleSet, len(tt))
	for _, tup := range tt {
		h, err := tup.Hash(types.Format_Default)
		if err != nil {
			panic(err)
		}
		s[h] = tup
	}
	return
}
