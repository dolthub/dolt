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
	"github.com/dolthub/dolt/go/store/hash"
)

func TestConcurrentGC(t *testing.T) {
	tests := []concurrentGCtest{
		{
			name: "smoke test",
			setup: client{queries: func(string, int) []string {
				return []string{
					"CREATE TABLE t (id int primary key)",
				}
			}},
			clients: []client{
				{queries: func(id string, i int) (queries []string) {
					return []string{
						fmt.Sprintf("INSERT INTO t VALUES (%d)", i),
						"SELECT COUNT(*) FROM t",
					}
				}},
			},
			iters: 100,
		},
		{
			name: "aaron's repro",
			skip: true,
			// create 32 branches
			setup: client{queries: func(string, int) []string {
				queries := []string{
					"CREATE TABLE t (id int primary key, val TEXT)",
					"CALL dcommit('-Am', 'new table t');",
				}
				for b := 0; b < 32; b++ {
					q := fmt.Sprintf("CALL dolt_checkout('-b', 'branch_%d');", b)
					queries = append(queries, q)
				}
				return queries
			}},
			// create a client for each branch
			clients: func() []client {
				cc := []client{{
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
			iters: 100,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.skip {
				t.Skip()
			}
			testConcurrentGC(t, test)
		})
	}
}

// concurrentGCtest tests concurrent GC
type concurrentGCtest struct {
	name    string
	skip    bool
	setup   client
	clients []client
	iters   int
}

type client struct {
	id      string
	queries func(id string, idx int) []string
}

func testConcurrentGC(t *testing.T, test concurrentGCtest) {
	require.NotZero(t, test.iters)
	ctx := context.Background()
	eng := setupConcurrencyTest(t, ctx)

	require.NoError(t, executeQueries(ctx, eng, 1, test.setup))
	eg, ctx := errgroup.WithContext(ctx)
	for _, c := range test.clients {
		c := c
		eg.Go(func() (err error) {
			err = executeQueries(ctx, eng, test.iters, c)
			if err != nil {
				panic(err)
			}
			return
		})
	}
	assert.NoError(t, eg.Wait())
}

func executeQueries(ctx context.Context, eng *engine.SqlEngine, iters int, c client) error {
	sess, err := eng.NewDoltSession(ctx, sql.NewBaseSession())
	if err != nil {
		return err
	}
	sctx := sql.NewContext(ctx, sql.WithSession(sess))
	sctx.SetCurrentDatabase(testDB)
	sctx.Session.SetClient(sql.Client{User: "root", Address: "%"})

	runQuery := func(sctx *sql.Context, eng *engine.SqlEngine, query string) (err error) {
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

	// generate and run |iters| batches of queries
	for i := 0; i < iters; i++ {
		for _, q := range c.queries(c.id, i) {
			err = runQuery(sctx, eng, q)
			if sql.ErrLockDeadlock.Is(err) {
				err = nil // allow serialization errors
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	// DB name matches dtestutils.CreateTestEnv()
	testDB = "dolt"
)

func setupConcurrencyTest(t *testing.T, ctx context.Context) (eng *engine.SqlEngine) {
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
