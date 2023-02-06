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
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestConcurrentGC(t *testing.T) {
	dprocedures.DoltGCFeatureFlag = true
	// Each test spawns concurrent clients execute queries, some clients
	// will trigger gc processes. When all clients finish, we run a final
	// gc process to validate that no dangling references remain.
	tests := []concurrentGCtest{
		{
			name:  "smoke test",
			setup: []string{"CREATE TABLE t (id int primary key)"},
			clients: []client{
				{
					id: "client",
					queries: func(id string, i int) (queries []string) {
						return []string{
							fmt.Sprintf("INSERT INTO t VALUES (%d)", i),
							"SELECT COUNT(*) FROM t",
						}
					}},
			},
		},
		{
			name: "aaron's repro",
			// create 32 branches
			setup: func() []string {
				queries := []string{
					"CREATE TABLE t (id int primary key, val TEXT)",
					"CALL dcommit('-Am', 'new table t');",
				}
				for b := 0; b < 32; b++ {
					q := fmt.Sprintf("CALL dolt_checkout('-b', 'branch_%d');", b)
					queries = append(queries, q)
				}
				return queries
			}(),
			// for each branch, create a single client that
			// writes only to that branch
			clients: func() []client {
				cc := []client{{
					id: "gc_client",
					queries: func(string, int) []string {
						return []string{"CALL dolt_gc();"}
					},
				}}
				for b := 0; b < 32; b++ {
					branch := fmt.Sprintf("branch_%d", b)
					cc = append(cc, client{
						id: branch,
						queries: func(id string, idx int) []string {
							q := fmt.Sprintf("INSERT INTO `%s/%s`.t VALUES (%d, '%s_%d')",
								testDB, id, idx, id, idx)
							return []string{q}
						}})
				}
				return cc
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testConcurrentGC(t, test)
		})
	}
}

// concurrentGCtest tests concurrent GC
type concurrentGCtest struct {
	name    string
	setup   []string
	clients []client
}

type client struct {
	id      string
	queries func(id string, idx int) []string
}

func testConcurrentGC(t *testing.T, test concurrentGCtest) {
	ctx := context.Background()
	eng := setupSqlEngine(t, ctx)
	err := runWithSqlSession(ctx, eng, func(sctx *sql.Context, eng *engine.SqlEngine) error {
		for _, q := range test.setup {
			if err := execQuery(sctx, eng, q); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	eg, ectx := errgroup.WithContext(ctx)
	for _, c := range test.clients {
		cl := c
		require.NotZero(t, cl.id)
		eg.Go(func() error {
			return runWithSqlSession(ectx, eng, func(sctx *sql.Context, eng *engine.SqlEngine) error {
				defer func() {
					if r := recover(); r != nil {
						//t.Logf("panic in client %s: %v", cl.id, r)
					}
				}()
				// generate and run 128 batches of queries
				for i := 0; i < 128; i++ {
					batch := cl.queries(cl.id, i)
					for _, q := range batch {
						qerr := execQuery(sctx, eng, q)
						if qerr != nil {
							// allow clients to error, but close connection
							// todo: restrict errors to dangling refs
							// t.Logf("error in client %s: %s", cl.id, qerr.Error())
							return nil
						}
					}
				}
				return nil
			})
		})
	}
	require.NoError(t, eg.Wait())

	// now run a full GC and assert we don't find dangling refs
	err = runWithSqlSession(ctx, eng, func(sctx *sql.Context, eng *engine.SqlEngine) (err error) {
		qq := []string{
			// ensure we have garbage to collect
			"CREATE TABLE garbage (val int)",
			"DROP TABLE garbage",
			"CALL dolt_gc()",
		}
		for _, q := range qq {
			if err = execQuery(sctx, eng, q); err != nil {
				return err
			}
		}
		return
	})
	require.NoError(t, err)
}

func runWithSqlSession(ctx context.Context, eng *engine.SqlEngine, cb func(sctx *sql.Context, eng *engine.SqlEngine) error) error {
	sess, err := eng.NewDoltSession(ctx, sql.NewBaseSession())
	if err != nil {
		return err
	}
	sctx := sql.NewContext(ctx, sql.WithSession(sess))
	sctx.SetCurrentDatabase(testDB)
	sctx.Session.SetClient(sql.Client{User: "root", Address: "%"})
	return cb(sctx, eng)
}

func execQuery(sctx *sql.Context, eng *engine.SqlEngine, query string) (err error) {
	_, iter, err := eng.Query(sctx, query)
	if err != nil {
		return err
	}
	defer func() {
		// tx commit
		if cerr := iter.Close(sctx); err == nil {
			err = cerr
		}
	}()
	for {
		_, err = iter.Next(sctx)
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return err
		}
	}
	return
}

const (
	// DB name matches dtestutils.CreateTestEnv()
	testDB = "dolt"
)

func setupSqlEngine(t *testing.T, ctx context.Context) (eng *engine.SqlEngine) {
	dEnv := dtestutils.CreateTestEnv()
	mrEnv, err := env.MultiEnvForDirectory(
		ctx,
		dEnv.Config.WriteableConfig(),
		dEnv.FS,
		dEnv.Version,
		dEnv.IgnoreLockFile,
		dEnv)
	if err != nil {
		panic(err)
	}

	eng, err = engine.NewSqlEngine(ctx, mrEnv, engine.FormatNull, &engine.SqlEngineConfig{
		InitialDb:    testDB,
		IsReadOnly:   false,
		PrivFilePath: "",
		ServerUser:   "root",
		ServerPass:   "",
		ServerHost:   "localhost",
		Autocommit:   true,
	})
	if err != nil {
		panic(err)
	}

	sqlCtx, err := eng.NewContext(ctx)
	require.NoError(t, err)
	sqlCtx.Session.SetClient(sql.Client{
		User: "root", Address: "%",
	})
	return
}

func TestGarbageCollection(t *testing.T) {
	require.True(t, true)
	assert.True(t, true)

	for _, gct := range gcTests {
		t.Run(gct.name, func(t *testing.T) {
			testGarbageCollection(t, gct)
		})
	}
}

type stage struct {
	commands     []testCommand
	preStageFunc func(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, prevRes interface{}) interface{}
}

type gcTest struct {
	name       string
	stages     []stage
	query      string
	expected   []sql.Row
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
		expected: []sql.Row{{int32(4)}, {int32(5)}, {int32(6)}},
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

	for _, c := range gcSetupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	var res interface{}
	for _, stage := range test.stages {
		res = stage.preStageFunc(ctx, t, dEnv.DoltDB, res)
		for _, c := range stage.commands {
			exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
			require.Equal(t, 0, exitCode)
		}
	}

	err := dEnv.DoltDB.GC(ctx)
	require.NoError(t, err)
	test.postGCFunc(ctx, t, dEnv.DoltDB, res)

	working, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	// assert all out rows are present after gc
	actual, err := sqle.ExecuteSelect(dEnv, working, test.query)
	require.NoError(t, err)
	assert.Equal(t, test.expected, actual)
}
