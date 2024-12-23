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

package migrate_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/migrate"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type migrationTest struct {
	name    string
	hook    setupHook
	setup   []string
	asserts []assertion
	err     string
}

// a setupHook performs special setup operations that cannot
// be performed via SQL statements (eg TEXT type PRIMARY KEY)
type setupHook func(context.Context, *env.DoltEnv) (*env.DoltEnv, error)

type assertion struct {
	query    string
	expected []sql.Row
}

func TestMigration(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip()
	}

	tests := []migrationTest{
		{
			name: "smoke test",
			setup: []string{
				"CREATE TABLE test (pk int primary key)",
				"INSERT INTO test VALUES (1),(2),(3)",
				"CALL dolt_add('.')",
				"CALL dolt_commit('-am', 'new table')",
			},
			asserts: []assertion{
				{
					query:    "SELECT * FROM test",
					expected: []sql.Row{{int32(1)}, {int32(2)}, {int32(3)}},
				},
				{
					query:    "SELECT count(*) FROM dolt_log",
					expected: []sql.Row{{int64(2)}},
				},
				{
					query:    "SELECT count(*) FROM `dolt/dolt_migrated_commits`.dolt_commit_mapping",
					expected: []sql.Row{{int64(2)}},
				},
			},
		},
		{
			name: "TEXT primary key, BLOB secondary key",
			hook: SetupHookRefKeys,
			setup: []string{
				// from setup hook:
				//   CREATE TABLE test (
				//     pk TEXT PRIMARY KEY,
				//     c0 int,
				//     c1 BLOB,
				//     INDEX blob_idx(c1),
				//   );
				"INSERT INTO test VALUES ('a', 2, 'a')",
				"CALL dolt_add('.')",
				"CALL dolt_commit('-am', 'new table')",
			},
			asserts: []assertion{
				{
					query:    "SELECT * FROM test",
					expected: []sql.Row{{"a", int32(2), []byte("a")}},
				},
				{
					query: "DESCRIBE test",
					expected: []sql.Row{
						{"pk", "varchar(16383)", "NO", "PRI", "NULL", ""},
						{"c0", "int", "YES", "", "NULL", ""},
						{"c1", "varbinary(16383)", "YES", "MUL", "NULL", ""},
					},
				},
			},
		},
		{
			name: "create more commits",
			setup: []string{
				"CREATE TABLE test (pk int primary key)",
				"INSERT INTO test VALUES (1),(2),(3)",
				"CALL dolt_commit('-Am', 'new table')",
				"INSERT INTO test VALUES (4)",
				"CALL dolt_commit('-am', 'added row 4')",
				"INSERT INTO test VALUES (5)",
				"CALL dolt_commit('-am', 'added row 5')",
			},
			asserts: []assertion{
				{
					query:    "SELECT count(*) FROM dolt_log",
					expected: []sql.Row{{int64(4)}},
				},
				{
					query:    "SELECT count(*) FROM `dolt/dolt_migrated_commits`.dolt_commit_mapping",
					expected: []sql.Row{{int64(4)}},
				},
				{
					query:    "SELECT count(*) FROM `dolt/dolt_migrated_commits`.dolt_commit_mapping WHERE new_commit_hash IN (SELECT commit_hash FROM dolt_log)",
					expected: []sql.Row{{int64(4)}},
				},
				{
					query:    "SELECT count(*) FROM `dolt/dolt_migrated_commits`.dolt_commit_mapping WHERE new_commit_hash NOT IN (SELECT commit_hash FROM dolt_log)",
					expected: []sql.Row{{int64(0)}},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			preEnv := setupMigrationTest(t, ctx, test)
			postEnv := runMigration(t, ctx, preEnv)
			root, err := postEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			for _, a := range test.asserts {
				actual, err := sqle.ExecuteSelect(postEnv, root, a.query)
				assert.NoError(t, err)
				assert.Equal(t, a.expected, actual)
			}
		})
	}
}

func setupMigrationTest(t *testing.T, ctx context.Context, test migrationTest) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()

	// run setup hook before other setup queries
	if test.hook != nil {
		var err error
		dEnv, err = test.hook(ctx, dEnv)
		require.NoError(t, err)
	}
	cliCtx, err := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, err)

	cmd := commands.SqlCmd{}
	for _, query := range test.setup {
		code := cmd.Exec(ctx, cmd.Name(), []string{"-q", query}, dEnv, cliCtx)
		require.Equal(t, 0, code)
	}
	return dEnv
}

func SetupHookRefKeys(ctx context.Context, dEnv *env.DoltEnv) (*env.DoltEnv, error) {
	pk, _ := schema.NewColumnWithTypeInfo("pk", 1, typeinfo.TextType, true, "", false, "", schema.NotNullConstraint{})
	c0, _ := schema.NewColumnWithTypeInfo("c0", 2, typeinfo.Int32Type, false, "", false, "")
	c1, _ := schema.NewColumnWithTypeInfo("c1", 3, typeinfo.BlobType, false, "", false, "")

	sch, err := schema.SchemaFromCols(schema.NewColCollection(pk, c0, c1))
	if err != nil {
		return nil, err
	}
	_, err = sch.Indexes().AddIndexByColNames("blob_idx", []string{"c1"}, nil, schema.IndexProperties{IsUserDefined: true})
	if err != nil {
		return nil, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return nil, err
	}
	root, err := doltdb.CreateEmptyTable(ctx, ws.WorkingRoot(), doltdb.TableName{Name: "test"}, sch)
	if err != nil {
		return nil, err
	}
	if err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(root)); err != nil {
		return nil, err
	}
	return dEnv, nil
}

func runMigration(t *testing.T, ctx context.Context, preEnv *env.DoltEnv) (postEnv *env.DoltEnv) {
	ddb, err := initTestMigrationDB(ctx)
	require.NoError(t, err)
	postEnv = &env.DoltEnv{
		Version:   preEnv.Version,
		Config:    preEnv.Config,
		RepoState: preEnv.RepoState,
		FS:        preEnv.FS,
		DoltDB:    ddb,
	}

	err = migrate.TraverseDAG(ctx, migrate.Environment{}, preEnv.DoltDB, postEnv.DoltDB)
	assert.NoError(t, err)
	return
}

func initTestMigrationDB(ctx context.Context) (*doltdb.DoltDB, error) {
	var db datas.Database
	storage := &chunks.MemoryStorage{}
	cs := storage.NewViewWithFormat("__DOLT__")
	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db = datas.NewTypesDatabase(vrw, ns)

	name, email := "user", "user@fake.horse"
	meta, err := datas.NewCommitMeta(name, email, "test migration")
	if err != nil {
		return nil, err
	}

	rv, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}

	ds, err := db.GetDataset(ctx, ref.NewInternalRef("migration").String())
	if err != nil {
		return nil, err
	}

	_, err = db.Commit(ctx, ds, rv.NomsValue(), datas.CommitOptions{Meta: meta})
	if err != nil {
		return nil, err
	}
	return doltdb.DoltDBFromCS(cs, ""), nil
}
