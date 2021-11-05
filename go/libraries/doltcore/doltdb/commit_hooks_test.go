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

package doltdb

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/test"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/stretchr/testify/assert"
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

	destDB, _ := LoadDoltDB(context.Background(), types.Format_Default, LocalDirDoltDB, filesys.LocalFS)

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

	ddb, _ := LoadDoltDB(context.Background(), types.Format_Default, LocalDirDoltDB, filesys.LocalFS)
	err = ddb.WriteEmptyRepo(context.Background(), "master", committerName, committerEmail)

	if err != nil {
		t.Fatal("Unexpected error creating empty repo", err)
	}

	// prepare a commit in the source repo
	cs, _ := NewCommitSpec("master")
	commit, err := ddb.Resolve(context.Background(), cs, nil)

	if err != nil {
		t.Fatal("Couldn't find commit")
	}

	meta, err := commit.GetCommitMeta()
	assert.NoError(t, err)

	if meta.Name != committerName || meta.Email != committerEmail {
		t.Error("Unexpected metadata")
	}

	root, err := commit.GetRootValue()

	assert.NoError(t, err)

	names, err := root.GetTableNames(context.Background())
	assert.NoError(t, err)
	if len(names) != 0 {
		t.Fatal("There should be no tables in empty db")
	}

	tSchema := createTestSchema(t)
	rowData, _ := createTestRowData(t, ddb.db, tSchema)
	tbl, err := CreateTestTable(ddb.db, tSchema, rowData)

	if err != nil {
		t.Fatal("Failed to create test table with data")
	}

	root, err = root.PutTable(context.Background(), "test", tbl)
	assert.NoError(t, err)

	valHash, err := ddb.WriteRootValue(context.Background(), root)
	assert.NoError(t, err)

	meta, err = NewCommitMeta(committerName, committerEmail, "Sample data")
	if err != nil {
		t.Error("Failed to commit")
	}

	// setup hook
	hook := NewPushOnWriteHook(destDB, tmpDir)
	ddb.SetCommitHooks(ctx, []datas.CommitHook{hook})

	t.Run("replicate to remote", func(t *testing.T) {
		srcCommit, err := ddb.Commit(context.Background(), valHash, ref.NewBranchRef(defaultBranch), meta)
		ds, err := ddb.db.GetDataset(ctx, "refs/heads/main")
		err = hook.Execute(ctx, ds, ddb.db)
		assert.NoError(t, err)

		cs, _ = NewCommitSpec(defaultBranch)
		destCommit, err := destDB.Resolve(context.Background(), cs, nil)

		srcHash, _ := srcCommit.HashOf()
		destHash, _ := destCommit.HashOf()
		assert.Equal(t, srcHash, destHash)
	})

	t.Run("replicate handle error logs to writer", func(t *testing.T) {
		var buffer = &bytes.Buffer{}
		err = hook.SetLogger(ctx, buffer)
		assert.NoError(t, err)

		msg := "prince charles is a vampire"
		hook.HandleError(ctx, errors.New(msg))

		assert.Equal(t, buffer.String(), msg)
	})
}

func TestLogHook(t *testing.T) {
	msg := []byte("hello")
	var err error
	t.Run("new log hook", func(t *testing.T) {
		ctx := context.Background()
		hook := NewLogHook(msg)
		var buffer = &bytes.Buffer{}
		err = hook.SetLogger(ctx, buffer)
		assert.NoError(t, err)
		hook.Execute(ctx, datas.Dataset{}, nil)
		assert.Equal(t, buffer.Bytes(), msg)
	})
}
