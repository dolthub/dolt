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
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestMerge(t *testing.T) {

	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY, c0 int);"}},
		{cmd.AddCmd{}, args{"."}},
		{cmd.CommitCmd{}, args{"-am", "created table test"}},
	}

	tests := []struct {
		name  string
		setup []testCommand

		query    string
		expected []sql.UntypedSqlRow
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
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM test",
			expected: []sql.UntypedSqlRow{
				{int32(1), int32(1)},
				{int32(2), int32(2)},
			},
		},
		{
			name: "three-way merge",
			setup: []testCommand{
				{cmd.BranchCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (11,11),(22,22);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on main"}},
				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM test",
			expected: []sql.UntypedSqlRow{
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
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added rows on main"}},
				{cmd.CheckoutCmd{}, args{"other"}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk varchar(120) primary key);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES ('x'),('y'),('z');"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM quiz ORDER BY pk",
			expected: []sql.UntypedSqlRow{
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
			defer dEnv.DoltDB.Close()

			for _, tc := range setupCommon {
				exit := tc.exec(t, ctx, dEnv)
				require.Equal(t, 0, exit)
			}
			for _, tc := range test.setup {
				exit := tc.exec(t, ctx, dEnv)
				require.Equal(t, 0, exit)
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			actRows, err := sqle.ExecuteSelect(dEnv, root, test.query)
			require.NoError(t, err)

			require.Equal(t, len(test.expected), len(actRows))
			for i := range test.expected {
				assert.Equal(t, test.expected[i], actRows[i])
			}
		})
	}
}

func TestMergeConflicts(t *testing.T) {

	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", "CREATE TABLE test (pk int PRIMARY KEY, c0 int);"}},
		{cmd.AddCmd{}, []string{"."}},
		{cmd.CommitCmd{}, args{"-am", "created table test"}},
	}

	tests := []struct {
		name  string
		setup []testCommand

		query    string
		expected []sql.UntypedSqlRow
	}{
		{
			name: "conflict on merge",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,11),(2,22);"}},
				{cmd.CommitCmd{}, args{"-am", "added the same rows on main"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM dolt_conflicts",
			expected: []sql.UntypedSqlRow{
				{"test", uint64(2)},
			},
		},
		{
			name: "conflict on merge, resolve with ours",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,11),(2,22);"}},
				{cmd.CommitCmd{}, args{"-am", "added the same rows on main"}},
				{cmd.MergeCmd{}, args{"other"}},
				{cnfcmds.ResolveCmd{}, args{"--ours", "test"}},
			},
			query: "SELECT * FROM test",
			expected: []sql.UntypedSqlRow{
				{int32(1), int32(11)},
				{int32(2), int32(22)},
			},
		},
		{
			name: "conflict on merge, no table in ancestor",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk int PRIMARY KEY, c0 int);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES (1,1),(2,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk int PRIMARY KEY, c0 int);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES (1,11),(2,22);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added the same rows on main"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "SELECT * FROM dolt_conflicts",
			expected: []sql.UntypedSqlRow{
				{"quiz", uint64(2)},
			},
		},
		{
			name: "conflict on merge, no table in ancestor, resolve with theirs",
			setup: []testCommand{
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk int PRIMARY KEY, c0 int);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES (1,1),(2,2);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added rows on other"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", "CREATE TABLE quiz (pk int PRIMARY KEY, c0 int);"}},
				{cmd.SqlCmd{}, args{"-q", "INSERT INTO quiz VALUES (1,11),(2,22);"}},
				{cmd.AddCmd{}, []string{"."}},
				{cmd.CommitCmd{}, args{"-am", "added the same rows on main"}},
				{cmd.MergeCmd{}, args{"other"}},
				{cnfcmds.ResolveCmd{}, args{"--theirs", "quiz"}},
			},
			query: "SELECT * FROM quiz",
			expected: []sql.UntypedSqlRow{
				{int32(1), int32(1)},
				{int32(2), int32(2)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtu.CreateTestEnv()
			defer dEnv.DoltDB.Close()

			for _, tc := range setupCommon {
				exit := tc.exec(t, ctx, dEnv)
				// allow merge to fail with conflicts
				if _, ok := tc.cmd.(cmd.MergeCmd); !ok {
					require.Equal(t, 0, exit)
				}
			}
			for _, tc := range test.setup {
				exit := tc.exec(t, ctx, dEnv)
				// allow merge to fail with conflicts
				if _, ok := tc.cmd.(cmd.MergeCmd); !ok {
					require.Equal(t, 0, exit)
				}
			}

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			actRows, err := sqle.ExecuteSelect(dEnv, root, test.query)
			require.NoError(t, err)

			require.Equal(t, len(test.expected), len(actRows))
			for i := range test.expected {
				assert.Equal(t, test.expected[i], actRows[i])
			}
		})
	}
}

const (
	concurrentScale   = 10_000
	concurrentIters   = 100
	concurrentThreads = 8
	concurrentTable   = "CREATE TABLE concurrent (" +
		"  id int NOT NULL," +
		"  c0 int NOT NULL," +
		"  c1 int NOT NULL," +
		"  PRIMARY KEY (id)," +
		"  KEY `idx0` (c0)," +
		"  KEY `idx1` (c1, c0)" +
		");"
)

// TestMergeConcurrency runs current merges via
// concurrent SQL transactions.
func TestMergeConcurrency(t *testing.T) {
	ctx := context.Background()
	dEnv := setupConcurrencyTest(t, ctx)
	defer dEnv.DoltDB.Close()
	_, eng := engineFromEnvironment(ctx, dEnv)

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < concurrentThreads; i++ {
		seed := i
		eg.Go(func() error {
			return runConcurrentTxs(ctx, eng, seed)
		})
	}
	assert.NoError(t, eg.Wait())
}

func runConcurrentTxs(ctx context.Context, eng *engine.SqlEngine, seed int) error {
	sess, err := eng.NewDoltSession(ctx, sql.NewBaseSession())
	if err != nil {
		return err
	}
	sctx := sql.NewContext(ctx, sql.WithSession(sess))
	sctx.SetCurrentDatabase("dolt")
	sctx.Session.SetClient(sql.Client{User: "root", Address: "%"})

	rnd := rand.New(rand.NewSource(int64(seed)))
	zipf := rand.NewZipf(rnd, 1.1, 1.0, concurrentScale)

	for i := 0; i < concurrentIters; i++ {
		if err := executeQuery(sctx, eng, "BEGIN"); err != nil {
			return err
		}

		id := zipf.Uint64()
		sum := fmt.Sprintf("SELECT sum(c0), sum(c1) "+
			"FROM concurrent WHERE id BETWEEN %d AND %d", id, id+10)
		update := fmt.Sprintf("UPDATE concurrent "+
			"SET c0 = c0 + %d, c1 = c1 + %d WHERE id = %d",
			seed, seed, id)

		if err := executeQuery(sctx, eng, sum); err != nil {
			return err
		}
		if err := executeQuery(sctx, eng, update); err != nil {
			return err
		}
		if err := executeQuery(sctx, eng, sum); err != nil {
			return err
		}
		if err := executeQuery(sctx, eng, "COMMIT"); err != nil {
			// allow serialization errors
			if !sql.ErrLockDeadlock.Is(err) {
				return err
			}
		}
	}
	return nil
}

func setupConcurrencyTest(t *testing.T, ctx context.Context) (dEnv *env.DoltEnv) {
	dEnv = dtu.CreateTestEnv()

	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)

	require.NoError(t, executeQuery(sqlCtx, eng, concurrentTable))
	require.NoError(t, executeQuery(sqlCtx, eng, generateTestData()))
	return
}

func engineFromEnvironment(ctx context.Context, dEnv *env.DoltEnv) (dbName string, eng *engine.SqlEngine) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		panic(err)
	}

	eng, err = engine.NewSqlEngine(ctx, mrEnv, &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		ServerHost: "localhost",
		Autocommit: true,
	})
	if err != nil {
		panic(err)
	}

	return mrEnv.GetFirstDatabase(), eng
}

func executeQuery(ctx *sql.Context, eng *engine.SqlEngine, query string) error {
	_, iter, _, err := eng.Query(ctx, query)
	if err != nil {
		return err
	}
	for {
		_, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return iter.Close(ctx) // tx commit
}

func generateTestData() string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO concurrent VALUES ")
	sb.WriteString("(0, 0, 0")
	for i := 1; i < concurrentScale; i++ {
		c0 := rand.Intn(concurrentScale)
		c1 := rand.Intn(concurrentScale)
		sb.WriteString("), (")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(", ")
		sb.WriteString(strconv.Itoa(c0))
		sb.WriteString(", ")
		sb.WriteString(strconv.Itoa(c1))
	}
	sb.WriteString(");")
	return sb.String()
}
