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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestKeylessMerge(t *testing.T) {

	tests := []struct {
		name     string
		setup    []testCommand
		expected keylessEntries
	}{
		{
			name: "fast-forward merge",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: []keylessEntry{
				{2, 1, 2},
				{1, 3, 4},
			},
		},
		{
			name: "3-way merge",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (5,6);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: []keylessEntry{
				{2, 1, 2},
				{1, 3, 4},
				{1, 5, 6},
			},
		},
		{
			name: "3-way merge with duplicates",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4), (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (5,6), (5,6);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			expected: []keylessEntry{
				{2, 1, 2},
				{2, 3, 4},
				{2, 5, 6},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtu.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			root, err = doltdb.CreateEmptyTable(ctx, root, doltdb.TableName{Name: tblName}, keylessSch)
			require.NoError(t, err)
			err = dEnv.UpdateWorkingRoot(ctx, root)
			require.NoError(t, err)
			cliCtx, err := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, err)

			for _, c := range test.setup {
				exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
				require.Equal(t, 0, exitCode)
			}

			root, err = dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
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
		conflicts conflictEntries

		oursExpected   keylessEntries
		theirsExpected keylessEntries
	}{
		{
			name: "identical parallel changes",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (3,4);"}},
				{cmd.CommitCmd{}, []string{"-am", "added rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: []conflictEntry{
				{
					base:   nil,
					ours:   &keylessEntry{1, 3, 4},
					theirs: &keylessEntry{1, 3, 4},
				},
			},
			oursExpected: []keylessEntry{
				{2, 1, 2},
				{1, 3, 4},
			},
			theirsExpected: []keylessEntry{
				{2, 1, 2},
				{1, 3, 4},
			},
		},
		{
			name: "asymmetric parallel deletes",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2),(1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "delete from noKey where (c1,c2) = (1,2) limit 1;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 1 row on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "delete from noKey where (c1,c2) = (1,2) limit 2;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 2 rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: []conflictEntry{
				{
					base:   &keylessEntry{4, 1, 2},
					ours:   &keylessEntry{2, 1, 2},
					theirs: &keylessEntry{3, 1, 2},
				},
			},
			oursExpected: []keylessEntry{
				{2, 1, 2},
			},
			theirsExpected: []keylessEntry{
				{3, 1, 2},
			},
		},
		{
			name: "asymmetric parallel updates",
			setup: []testCommand{
				{cmd.SqlCmd{}, []string{"-q", "insert into noKey values (1,2),(1,2),(1,2),(1,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, []string{"-am", "added rows"}},
				{cmd.CheckoutCmd{}, []string{"-b", "other"}},
				{cmd.SqlCmd{}, []string{"-q", "update noKey set c2 = 9 limit 1;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 1 row on other"}},
				{cmd.CheckoutCmd{}, []string{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, []string{"-q", "update noKey set c2 = 9 limit 2;"}},
				{cmd.CommitCmd{}, []string{"-am", "deleted 2 rows on main"}},
				{cmd.MergeCmd{}, []string{"other"}},
			},
			conflicts: []conflictEntry{
				{
					base:   &keylessEntry{4, 1, 2},
					ours:   &keylessEntry{2, 1, 2},
					theirs: &keylessEntry{3, 1, 2},
				},
				{
					base:   nil,
					ours:   &keylessEntry{2, 1, 9},
					theirs: &keylessEntry{1, 1, 9},
				},
			},
			oursExpected: []keylessEntry{
				{2, 1, 2},
				{2, 1, 9},
			},
			theirsExpected: []keylessEntry{
				{3, 1, 2},
				{1, 1, 9},
			},
		},
	}

	setupTest := func(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, cc []testCommand) {
		root, err := dEnv.WorkingRoot(ctx)
		require.NoError(t, err)
		root, err = doltdb.CreateEmptyTable(ctx, root, doltdb.TableName{Name: tblName}, keylessSch)
		require.NoError(t, err)
		err = dEnv.UpdateWorkingRoot(ctx, root)
		require.NoError(t, err)
		cliCtx, err := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
		require.NoError(t, err)

		for _, c := range cc {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
			// allow merge to fail with conflicts
			if _, ok := c.cmd.(cmd.MergeCmd); !ok {
				require.Equal(t, 0, exitCode)
			}
		}
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()
			setupTest(t, ctx, dEnv, test.setup)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
			require.NoError(t, err)
			assertConflicts(t, ctx, tbl, test.conflicts)
		})

		// conflict resolution

		t.Run(test.name+"_resolved_ours", func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()

			setupTest(t, ctx, dEnv, test.setup)
			cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, verr)

			resolve := cnfcmds.ResolveCmd{}
			args := []string{"--ours", tblName}
			exitCode := resolve.Exec(ctx, resolve.Name(), args, dEnv, cliCtx)
			require.Equal(t, 0, exitCode)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
			require.NoError(t, err)

			assertKeylessRows(t, ctx, tbl, test.oursExpected)
		})
		t.Run(test.name+"_resolved_theirs", func(t *testing.T) {
			dEnv := dtu.CreateTestEnv()
			defer dEnv.DoltDB(ctx).Close()

			setupTest(t, ctx, dEnv, test.setup)
			cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
			require.NoError(t, verr)

			resolve := cnfcmds.ResolveCmd{}
			args := []string{"--theirs", tblName}
			exitCode := resolve.Exec(ctx, resolve.Name(), args, dEnv, cliCtx)
			require.Equal(t, 0, exitCode)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: tblName})
			require.NoError(t, err)

			assertKeylessRows(t, ctx, tbl, test.theirsExpected)
		})
	}
}

func assertConflicts(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected conflictEntries) {
	if types.IsFormat_DOLT(tbl.Format()) {
		assertProllyConflicts(t, ctx, tbl, expected)
		return
	}
	assertNomsConflicts(t, ctx, tbl, expected)
}

func assertProllyConflicts(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected conflictEntries) {
	artIdx, err := tbl.GetArtifacts(ctx)
	require.NoError(t, err)
	artM := durable.ProllyMapFromArtifactIndex(artIdx)

	itr, err := artM.IterAllConflicts(ctx)
	require.NoError(t, err)

	expectedSet := expected.toConflictSet()

	var c int
	var h [16]byte
	for {
		conf, err := itr.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		c++

		ours := mustGetRowValueFromTable(t, ctx, tbl, conf.Key)
		theirs := mustGetRowValueFromRootIsh(t, ctx, tbl.ValueReadWriter(), tbl.NodeStore(), conf.TheirRootIsh, tblName, conf.Key)
		base := mustGetRowValueFromRootIsh(t, ctx, tbl.ValueReadWriter(), tbl.NodeStore(), conf.Metadata.BaseRootIsh, tblName, conf.Key)

		copy(h[:], conf.Key.GetField(0))
		expectedConf, ok := expectedSet[h]
		require.True(t, ok)

		if expectedConf.base != nil {
			_, value := expectedConf.base.HashAndValue()
			require.Equal(t, valDesc.Format(value), valDesc.Format(base))
		}
		if expectedConf.ours != nil {
			_, value := expectedConf.ours.HashAndValue()
			require.Equal(t, valDesc.Format(value), valDesc.Format(ours))
		}
		if expectedConf.theirs != nil {
			_, value := expectedConf.theirs.HashAndValue()
			require.Equal(t, valDesc.Format(value), valDesc.Format(theirs))
		}
	}

	require.Equal(t, len(expected), c)

}

func assertNomsConflicts(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected conflictEntries) {
	_, confIdx, err := tbl.GetConflicts(ctx)
	require.NoError(t, err)
	conflicts := durable.NomsMapFromConflictIndex(confIdx)

	assert.True(t, conflicts.Len() > 0)
	assert.Equal(t, int(conflicts.Len()), len(expected))

	expectedSet := expected.toTupleSet()

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
		exp, ok := expectedSet[h]
		assert.True(t, ok)
		assert.True(t, exp.Equals(act))
	}
}

func mustGetRowValueFromTable(t *testing.T, ctx context.Context, tbl *doltdb.Table, key val.Tuple) val.Tuple {
	idx, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	m := durable.ProllyMapFromIndex(idx)

	var value val.Tuple
	err = m.Get(ctx, key, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	require.NoError(t, err)

	return value
}

func mustGetRowValueFromRootIsh(t *testing.T, ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, rootIsh hash.Hash, tblName string, key val.Tuple) val.Tuple {
	rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, rootIsh)
	require.NoError(t, err)
	tbl, ok, err := rv.GetTable(ctx, doltdb.TableName{Name: tblName})
	require.NoError(t, err)
	require.True(t, ok)

	return mustGetRowValueFromTable(t, ctx, tbl, key)
}

// |expected| is a tupleSet to compensate for random storage order
func assertKeylessRows(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected keylessEntries) {
	if types.IsFormat_DOLT(tbl.Format()) {
		assertKeylessProllyRows(t, ctx, tbl, expected)
		return
	}

	assertKeylessNomsRows(t, ctx, tbl, expected)
}

func assertKeylessProllyRows(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected []keylessEntry) {
	idx, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	m := durable.ProllyMapFromIndex(idx)

	expectedSet := mustHash128Set(expected...)

	itr, err := m.IterAll(ctx)
	require.NoError(t, err)

	var c int
	var h [16]byte
	for {
		hashId, value, err := itr.Next(ctx)
		if err == io.EOF {
			break
		}
		c++
		require.NoError(t, err)
		copy(h[:], hashId.GetField(0))
		expectedVal, ok := expectedSet[h]
		assert.True(t, ok)
		assert.Equal(t, valDesc.Format(expectedVal), valDesc.Format(value))
	}

	require.Equal(t, len(expected), c)
}

func assertKeylessNomsRows(t *testing.T, ctx context.Context, tbl *doltdb.Table, expected keylessEntries) {
	rowData, err := tbl.GetNomsRowData(ctx)
	require.NoError(t, err)

	assert.Equal(t, int(rowData.Len()), len(expected))

	expectedSet := expected.toTupleSet()

	actual, err := rowData.Iterator(ctx)
	require.NoError(t, err)
	for {
		_, act, err := actual.Next(ctx)
		if act == nil {
			break
		}
		assert.NoError(t, err)
		h, err := act.Hash(types.Format_Default)
		assert.NoError(t, err)
		exp, ok := expectedSet[h]
		assert.True(t, ok)
		assert.True(t, exp.Equals(act))
	}
}

const tblName = "noKey"

var keylessSch = dtu.MustSchema(
	schema.NewColumn("c1", 1, types.IntKind, false),
	schema.NewColumn("c2", 2, types.IntKind, false),
)
var c1Tag = types.Uint(1)
var c2Tag = types.Uint(2)
var cardTag = types.Uint(schema.KeylessRowCardinalityTag)

var valDesc = val.NewTupleDescriptor(val.Type{Enc: val.Uint64Enc}, val.Type{Enc: val.Int64Enc, Nullable: true}, val.Type{Enc: val.Int64Enc, Nullable: true})
var valBld = val.NewTupleBuilder(valDesc)
var sharePool = pool.NewBuffPool()

type keylessEntries []keylessEntry
type keylessEntry struct {
	card int
	c1   int
	c2   int
}

func (e keylessEntries) toTupleSet() tupleSet {
	tups := make([]types.Tuple, len(e))
	for i, t := range e {
		tups[i] = t.ToNomsTuple()
	}
	return mustTupleSet(tups...)
}

func (e keylessEntry) ToNomsTuple() types.Tuple {
	return dtu.MustTuple(cardTag, types.Uint(e.card), c1Tag, types.Int(e.c1), c2Tag, types.Int(e.c2))
}

func (e keylessEntry) HashAndValue() ([]byte, val.Tuple) {
	valBld.PutUint64(0, uint64(e.card))
	valBld.PutInt64(1, int64(e.c1))
	valBld.PutInt64(2, int64(e.c2))

	value := valBld.Build(sharePool)
	hashTup := val.HashTupleFromValue(sharePool, value)
	return hashTup.GetField(0), value
}

type conflictSet map[[16]byte]conflictEntry
type conflictEntries []conflictEntry
type conflictEntry struct {
	base, ours, theirs *keylessEntry
}

func (e conflictEntries) toConflictSet() conflictSet {
	s := make(conflictSet, len(e))
	for _, t := range e {
		s[t.Key()] = t
	}
	return s
}

func (e conflictEntries) toTupleSet() tupleSet {
	tups := make([]types.Tuple, len(e))
	for i, t := range e {
		tups[i] = t.ToNomsTuple()
	}
	return mustTupleSet(tups...)
}

func (e conflictEntry) Key() (h [16]byte) {
	if e.base != nil {
		h2, _ := e.base.HashAndValue()
		copy(h[:], h2[:])
		return
	}
	if e.ours != nil {
		h2, _ := e.ours.HashAndValue()
		copy(h[:], h2[:])
		return
	}
	if e.theirs != nil {
		h2, _ := e.theirs.HashAndValue()
		copy(h[:], h2[:])
		return
	}

	return
}

func (e conflictEntry) ToNomsTuple() types.Tuple {
	var b, o, t types.Value = types.NullValue, types.NullValue, types.NullValue
	if e.base != nil {
		b = e.base.ToNomsTuple()
	}
	if e.ours != nil {
		o = e.ours.ToNomsTuple()
	}
	if e.theirs != nil {
		t = e.theirs.ToNomsTuple()
	}
	return dtu.MustTuple(b, o, t)
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

type hash128Set map[[16]byte]val.Tuple

func mustHash128Set(entries ...keylessEntry) (s hash128Set) {
	var h [16]byte
	s = make(hash128Set, len(entries))

	for _, e := range entries {
		h2, value := e.HashAndValue()
		copy(h[:], h2)
		s[h] = value
	}

	return s
}
