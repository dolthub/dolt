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
	"errors"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestGarbageCollection(t *testing.T) {
	require.True(t, true)
	assert.True(t, true)

	for _, gct := range gcTests {
		t.Run(gct.name, func(t *testing.T) {
			testGarbageCollection(t, gct)
		})
	}

	t.Run("HasCacheDataCorruption", testGarbageCollectionHasCacheDataCorruptionBugFix)
}

type stage struct {
	commands     []testCommand
	preStageFunc func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{}) interface{}
}

type gcTest struct {
	name       string
	stages     []stage
	query      string
	expected   []sql.UntypedSqlRow
	postGCFunc func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{})
}

var gcTests = []gcTest{
	{
		name: "gc test",
		stages: []stage{
			{
				preStageFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, i interface{}) interface{} {
					return nil
				},
				commands: []testCommand{
					{commands.CheckoutCmd{}, []string{"-b", "temp"}},
					{commands.SqlCmd{}, []string{"-q", "INSERT INTO test VALUES (0),(1),(2);"}},
					{commands.AddCmd{}, []string{"."}},
					{commands.CommitCmd{}, []string{"-m", "commit"}},
				},
			},
			{
				preStageFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, i interface{}) interface{} {
					cm, err := ddb.ResolveCommitRef(ctx, ref.NewBranchRef("temp"))
					require.NoError(t, err)
					h, err := cm.HashOf()
					require.NoError(t, err)
					cs, err := doltdb.NewCommitSpec(h.String())
					require.NoError(t, err)
					_, err = ddb.Resolve(ctx, cs, nil)
					require.NoError(t, err)
					return h
				},
				commands: []testCommand{
					{commands.CheckoutCmd{}, []string{env.DefaultInitBranch}},
					{commands.BranchCmd{}, []string{"-D", "temp"}},
					{commands.SqlCmd{}, []string{"-q", "INSERT INTO test VALUES (4),(5),(6);"}},
				},
			},
		},
		query:    "select * from test;",
		expected: []sql.UntypedSqlRow{{int32(4)}, {int32(5)}, {int32(6)}},
		postGCFunc: func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{}) {
			h := prevRes.(hash.Hash)
			cs, err := doltdb.NewCommitSpec(h.String())
			require.NoError(t, err)
			_, err = ddb.Resolve(ctx, cs, nil)
			require.Error(t, err)
		},
	},
}

var gcSetupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "CREATE TABLE test (pk int PRIMARY KEY)"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "created test table"}},
}

func testGarbageCollection(t *testing.T, test gcTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()

	cliCtx, verr := commands.NewArgFreeCliContext(ctx, dEnv)
	require.NoError(t, verr)

	for _, c := range gcSetupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	var res interface{}
	for _, stage := range test.stages {
		res = stage.preStageFunc(ctx, t, dEnv.DoltDB, res)
		for _, c := range stage.commands {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
			require.Equal(t, 0, exitCode)
		}
	}

	err := dEnv.DoltDB.GC(ctx, types.GCModeDefault, nil)
	require.NoError(t, err)
	test.postGCFunc(ctx, t, dEnv.DoltDB, res)

	working, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	// assert all out rows are present after gc
	actual, err := sqle.ExecuteSelect(dEnv, working, test.query)
	require.NoError(t, err)
	assert.Equal(t, test.expected, actual)
}

// In September 2023, we found a failure to handle the `hasCache` in
// `*NomsBlockStore` appropriately while cleaning up a memtable into which
// dangling references had been written could result in writing chunks to a
// database which referenced non-existent chunks.
//
// The general pattern was to get new chunk addresses into the hasCache, but
// not written to the store, and then to have an incoming chunk add a reference
// to missing chunk. At that time, we would clear the memtable, since it had
// invalid chunks in it, but we wouldn't purge the hasCache. Later writes which
// attempted to reference the chunks which had made it into the hasCache would
// succeed.
//
// One such concrete pattern for doing this is implemented below. We do:
//
// 1) Put a new chunk to the database -- C1.
//
// 2) Run a GC.
//
// 3) Put a new chunk to the database -- C2.
//
// 4) Call NBS.Commit() with a stale last hash.Hash. This causes us to cache C2
// as present in the store, but it does not get written to disk, because the
// optimistic concurrency control on the value of the current root hash fails.
//
// 5) Put a chunk referencing C1 to the database -- R1.
//
// 5) Call NBS.Commit(). This causes ErrDanglingRef. C1 was written before the
// GC and is no longer in the store. C2 is also cleared from the pending write
// set.
//
// 6) Put a chunk referencing C2 to the database -- R2.
//
// 7) Call NBS.Commit(). This should fail, since R2 references C2 and C2 is not
// in the store. However, C2 is in the cache as a result of step #4, and so
// this does not fail. R2 gets written to disk with a dangling reference to C2.
func testGarbageCollectionHasCacheDataCorruptionBugFix(t *testing.T) {
	ctx := context.Background()

	d, err := os.MkdirTemp(t.TempDir(), "hascachetest-")
	require.NoError(t, err)

	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, "file://"+d, filesys.LocalFS)
	require.NoError(t, err)
	defer ddb.Close()

	err = ddb.WriteEmptyRepo(ctx, "main", "Aaron Son", "aaron@dolthub.com")
	require.NoError(t, err)

	root, err := ddb.NomsRoot(ctx)
	require.NoError(t, err)

	ns := ddb.NodeStore()

	c1 := newIntMap(t, ctx, ns, 1, 1)
	_, err = ns.Write(ctx, c1.Node())
	require.NoError(t, err)

	err = ddb.GC(ctx, types.GCModeDefault, nil)
	require.NoError(t, err)

	c2 := newIntMap(t, ctx, ns, 2, 2)
	_, err = ns.Write(ctx, c2.Node())
	require.NoError(t, err)

	success, err := ddb.CommitRoot(ctx, c2.HashOf(), c2.HashOf())
	require.NoError(t, err)
	require.False(t, success, "committing the root with a last hash which does not match the current root must fail")

	r1 := newAddrMap(t, ctx, ns, "r1", c1.HashOf())
	_, err = ns.Write(ctx, r1.Node())
	require.NoError(t, err)

	success, err = ddb.CommitRoot(ctx, root, root)
	require.True(t, errors.Is(err, nbs.ErrDanglingRef), "committing a reference to just-collected c1 must fail with ErrDanglingRef")

	r2 := newAddrMap(t, ctx, ns, "r2", c2.HashOf())
	_, err = ns.Write(ctx, r2.Node())
	require.NoError(t, err)

	success, err = ddb.CommitRoot(ctx, root, root)
	require.True(t, errors.Is(err, nbs.ErrDanglingRef), "committing a reference to c2, which was erased with the ErrDanglingRef above, must also fail with ErrDanglingRef")
}

func newIntMap(t *testing.T, ctx context.Context, ns tree.NodeStore, k, v int8) prolly.Map {
	desc := val.NewTupleDescriptor(val.Type{
		Enc:      val.Int8Enc,
		Nullable: false,
	})

	tb := val.NewTupleBuilder(desc)
	tb.PutInt8(0, k)
	keyTuple := tb.Build(ns.Pool())

	tb.PutInt8(0, v)
	valueTuple := tb.Build(ns.Pool())

	m, err := prolly.NewMapFromTuples(ctx, ns, desc, desc, keyTuple, valueTuple)
	require.NoError(t, err)
	return m
}

func newAddrMap(t *testing.T, ctx context.Context, ns tree.NodeStore, key string, h hash.Hash) prolly.AddressMap {
	m, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)

	editor := m.Editor()
	err = editor.Add(ctx, key, h)
	require.NoError(t, err)

	m, err = editor.Flush(ctx)
	require.NoError(t, err)

	return m
}
