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

package sqle

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/test"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const defaultBranch = "main"

func TestPushOnWriteHook(t *testing.T) {
	ctx := context.Background()

	// destination repo
	testDir, err := test.ChangeToTestDir("TestReplicationDest")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	committerName := "Bill Billerson"
	committerEmail := "bigbillieb@fake.horse"

	tmpDir := filepath.Join(testDir, dbfactory.DoltDataDir)
	err = filesys.LocalFS.MkDirs(tmpDir)

	if err != nil {
		t.Fatal("Failed to create noms directory")
	}

	destDB, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)

	// source repo
	testDir, err = test.ChangeToTestDir("TestReplicationSource")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	tmpDir = filepath.Join(testDir, dbfactory.DoltDataDir)
	err = filesys.LocalFS.MkDirs(tmpDir)

	if err != nil {
		t.Fatal("Failed to create noms directory")
	}

	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)
	err = ddb.WriteEmptyRepo(context.Background(), "main", committerName, committerEmail)

	if err != nil {
		t.Fatal("Unexpected error creating empty repo", err)
	}

	// prepare a commit in the source repo
	cs, _ := doltdb.NewCommitSpec("main")
	optCmt, err := ddb.Resolve(context.Background(), cs, nil)
	if err != nil {
		t.Fatal("Couldn't find commit")
	}
	commit, ok := optCmt.ToCommit()
	assert.True(t, ok)

	meta, err := commit.GetCommitMeta(context.Background())
	assert.NoError(t, err)

	if meta.Name != committerName || meta.Email != committerEmail {
		t.Fatal("Unexpected metadata")
	}

	root, err := commit.GetRootValue(context.Background())

	assert.NoError(t, err)

	names, err := root.GetTableNames(context.Background(), doltdb.DefaultSchemaName, true)
	assert.NoError(t, err)
	if len(names) != 0 {
		t.Fatal("There should be no tables in empty db")
	}

	tSchema := createTestSchema(t)
	rowData := createTestRowData(t, ddb.ValueReadWriter(), ddb.NodeStore(), tSchema)
	tbl, err := createHooksTestTable(ddb.ValueReadWriter(), ddb.NodeStore(), tSchema, rowData)

	if err != nil {
		t.Fatal("Failed to create test table with data")
	}

	root, err = root.PutTable(context.Background(), doltdb.TableName{Name: "test"}, tbl)
	assert.NoError(t, err)

	r, valHash, err := ddb.WriteRootValue(context.Background(), root)
	assert.NoError(t, err)
	root = r

	meta, err = datas.NewCommitMeta(committerName, committerEmail, "Sample data")
	if err != nil {
		t.Error("Failed to commit")
	}

	logger := &bytes.Buffer{}

	// setup hook
	hook := NewPushOnWriteHook(tmpDir, logger)
	hook.destDb = destDB
	ddb.PrependCommitHooks(ctx, hook)

	t.Run("replicate to remote", func(t *testing.T) {
		srcCommit, err := ddb.Commit(context.Background(), valHash, ref.NewBranchRef(defaultBranch), meta)
		require.NoError(t, err)

		ds, err := doltdb.HackDatasDatabaseFromDoltDB(ddb).GetDataset(ctx, "refs/heads/main")
		require.NoError(t, err)

		_, err = hook.Execute(ctx, ds, ddb)
		require.NoError(t, err)

		cs, _ = doltdb.NewCommitSpec(defaultBranch)
		optCmt, err := destDB.Resolve(context.Background(), cs, nil)
		require.NoError(t, err)
		destCommit, ok := optCmt.ToCommit()
		require.True(t, ok)

		srcHash, _ := srcCommit.HashOf()
		destHash, _ := destCommit.HashOf()
		assert.Equal(t, srcHash, destHash)
	})
}

func TestLogHook(t *testing.T) {
	msg := []byte("hello")
	t.Run("new log hook", func(t *testing.T) {
		ctx := context.Background()
		var buffer = &bytes.Buffer{}
		hook := NewLogHook(msg, buffer)
		hook.Execute(ctx, datas.Dataset{}, nil)
		assert.Equal(t, msg, buffer.Bytes())
	})
}

func TestAsyncPushOnWrite(t *testing.T) {
	ctx := context.Background()

	// destination repo
	testDir, err := test.ChangeToTestDir("TestReplicationDest")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	committerName := "Bill Billerson"
	committerEmail := "bigbillieb@fake.horse"

	tmpDir := filepath.Join(testDir, dbfactory.DoltDataDir)
	err = filesys.LocalFS.MkDirs(tmpDir)

	if err != nil {
		t.Fatal("Failed to create noms directory")
	}

	destDB, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)

	// source repo
	testDir, err = test.ChangeToTestDir("TestReplicationSource")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	tmpDir = filepath.Join(testDir, dbfactory.DoltDataDir)
	err = filesys.LocalFS.MkDirs(tmpDir)

	if err != nil {
		t.Fatal("Failed to create noms directory")
	}

	ddb, _ := doltdb.LoadDoltDB(context.Background(), types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)
	err = ddb.WriteEmptyRepo(context.Background(), "main", committerName, committerEmail)

	if err != nil {
		t.Fatal("Unexpected error creating empty repo", err)
	}

	t.Run("replicate to remote", func(t *testing.T) {
		bThreads := sql.NewBackgroundThreads()
		defer bThreads.Shutdown()
		hook, runThreads := NewAsyncPushOnWriteHook(tmpDir, &buffer.Buffer{})
		hook.destDb = destDB
		require.NotNil(t, hook)
		require.NotNil(t, runThreads)
		runThreads(bThreads, func(ctx context.Context) (*sql.Context, error) {
			return sql.NewContext(ctx), nil
		})

		for i := 0; i < 200; i++ {
			cs, _ := doltdb.NewCommitSpec("main")
			optCmt, err := ddb.Resolve(context.Background(), cs, nil)
			if err != nil {
				t.Fatal("Couldn't find commit")
			}
			commit, ok := optCmt.ToCommit()
			assert.True(t, ok)

			meta, err := commit.GetCommitMeta(context.Background())
			assert.NoError(t, err)

			if meta.Name != committerName || meta.Email != committerEmail {
				t.Error("Unexpected metadata")
			}

			root, err := commit.GetRootValue(context.Background())

			assert.NoError(t, err)

			tSchema := createTestSchema(t)
			rowData, err := durable.NewEmptyPrimaryIndex(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), tSchema)
			require.NoError(t, err)
			tbl, err := createHooksTestTable(ddb.ValueReadWriter(), ddb.NodeStore(), tSchema, rowData)
			require.NoError(t, err)

			if err != nil {
				t.Fatal("Failed to create test table with data")
			}

			root, err = root.PutTable(context.Background(), doltdb.TableName{Name: "test"}, tbl)
			assert.NoError(t, err)

			r, valHash, err := ddb.WriteRootValue(context.Background(), root)
			assert.NoError(t, err)
			root = r

			meta, err = datas.NewCommitMeta(committerName, committerEmail, "Sample data")
			if err != nil {
				t.Error("Failed to create CommitMeta")
			}

			_, err = ddb.Commit(context.Background(), valHash, ref.NewBranchRef(defaultBranch), meta)
			require.NoError(t, err)
			ds, err := doltdb.HackDatasDatabaseFromDoltDB(ddb).GetDataset(ctx, "refs/heads/main")
			require.NoError(t, err)
			_, err = hook.Execute(ctx, ds, ddb)
			require.NoError(t, err)
		}
	})

	t.Run("does not over replicate branch delete", func(t *testing.T) {
		// We used to have a bug where a branch delete would be
		// replicated over and over again endlessly.

		// The test construction here is that we put a counting commit
		// hook on *destDB*.  Then we call the async push hook as if we
		// need to replicate certain head updates.  We call once for a
		// branch that does exist and once for a branch which does not
		// exist. Calling with a branch which does not exist looks the
		// same as the call which is made after a branch delete.

		counts := &countingCommitHook{make(map[string]int)}
		destDB.PrependCommitHooks(context.Background(), counts)

		bThreads := sql.NewBackgroundThreads()
		hook, runThreads := NewAsyncPushOnWriteHook(tmpDir, &buffer.Buffer{})
		hook.destDb = destDB
		runThreads(bThreads, func(ctx context.Context) (*sql.Context, error) {
			return sql.NewContext(ctx), nil
		})

		// Pretend we replicate a HEAD which does exist.
		ds, err := doltdb.HackDatasDatabaseFromDoltDB(ddb).GetDataset(ctx, "refs/heads/main")
		require.NoError(t, err)
		_, err = hook.Execute(ctx, ds, ddb)
		require.NoError(t, err)

		// Pretend we replicate a HEAD which does not exist, i.e., a branch delete.
		ds, err = doltdb.HackDatasDatabaseFromDoltDB(ddb).GetDataset(ctx, "refs/heads/does_not_exist")
		require.NoError(t, err)
		_, err = hook.Execute(ctx, ds, ddb)
		require.NoError(t, err)

		// Wait a bit for background thread to fire, in case it is
		// going to betray us. TODO: Structure AsyncPushOnWriteHook to
		// be more testable, so we do not have to rely on
		// non-determinstic goroutine scheduling and best-effort sleeps
		// to observe the potential failure here.
		time.Sleep(10 * time.Second)

		// Shutdown thread to get final replication if necessary.
		bThreads.Shutdown()

		// If all went well, the branch delete was executed exactly once.
		require.Equal(t, 1, counts.counts["refs/heads/does_not_exist"])
	})
}

var _ doltdb.CommitHook = (*countingCommitHook)(nil)

type countingCommitHook struct {
	// The number of times Execute() got called for given dataset.
	counts map[string]int
}

func (c *countingCommitHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	c.counts[ds.ID()] += 1
	return nil, nil
}

func (c *countingCommitHook) ExecuteForWorkingSets() bool {
	return false
}

const (
	idTag        = 0
	firstTag     = 1
	lastTag      = 2
	isMarriedTag = 3
	ageTag       = 4
	emptySchTag  = 5
)
const testSchemaIndexName = "idx_name"
const testSchemaIndexAge = "idx_age"

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestSchema(t *testing.T) schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.UintKind, false),
		schema.NewColumn("empty", emptySchTag, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColTags(testSchemaIndexName, []uint64{firstTag, lastTag}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColTags(testSchemaIndexAge, []uint64{ageTag}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	return sch
}

func createTestRowData(t *testing.T, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema) durable.Index {
	if types.Format_Default == types.Format_DOLT {
		idx, err := durable.NewEmptyPrimaryIndex(context.Background(), vrw, ns, sch)
		require.NoError(t, err)
		return idx
	}

	t.Fatal("unsupported format")
	return nil
}

func createHooksTestTable(vrw types.ValueReadWriter, ns tree.NodeStore, tSchema schema.Schema, rowData durable.Index) (*doltdb.Table, error) {
	tbl, err := doltdb.NewTable(context.Background(), vrw, ns, tSchema, rowData, nil, nil)

	if err != nil {
		return nil, err
	}

	return tbl, nil
}
