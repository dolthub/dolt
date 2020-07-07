// Copyright 2019 Liquidata, Inc.
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
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/test"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	idTag        = 0
	firstTag     = 1
	lastTag      = 2
	isMarriedTag = 3
	ageTag       = 4
	emptyTag     = 5
)
const testSchemaIndexName = "idx_name"
const testSchemaIndexAge = "idx_age"

func createTestSchema(t *testing.T) schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.UintKind, false),
		schema.NewColumn("empty", emptyTag, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)
	_, err := sch.Indexes().AddIndexByColTags(testSchemaIndexName, []uint64{firstTag, lastTag}, schema.IndexProperties{IsUnique: false, IsHidden: false, Comment: ""})
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColTags(testSchemaIndexAge, []uint64{ageTag}, schema.IndexProperties{IsUnique: false, IsHidden: false, Comment: ""})
	require.NoError(t, err)
	return sch
}

func TestEmptyInMemoryRepoCreation(t *testing.T) {
	ddb, err := LoadDoltDB(context.Background(), types.Format_7_18, InMemDoltDB)

	if err != nil {
		t.Fatal("Failed to load db")
	}

	err = ddb.WriteEmptyRepo(context.Background(), "Bill Billerson", "bigbillieb@fake.horse")

	if err != nil {
		t.Fatal("Unexpected error creating empty repo", err)
	}

	cs, _ := NewCommitSpec("master", "")
	commit, err := ddb.Resolve(context.Background(), cs, nil)

	if err != nil {
		t.Fatal("Could not find commit")
	}

	h, err := commit.HashOf()
	assert.NoError(t, err)
	cs2, _ := NewCommitSpec(h.String(), "")
	_, err = ddb.Resolve(context.Background(), cs2, nil)

	if err != nil {
		t.Fatal("Failed to get commit by hash")
	}
}

func TestLoadNonExistentLocalFSRepo(t *testing.T) {
	_, err := test.ChangeToTestDir("TestLoadRepo")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	ddb, err := LoadDoltDB(context.Background(), types.Format_7_18, LocalDirDoltDB)
	assert.Nil(t, ddb, "Should return nil when loading a non-existent data dir")
	assert.Error(t, err, "Should see an error here")
}

func TestLoadBadLocalFSRepo(t *testing.T) {
	testDir, err := test.ChangeToTestDir("TestLoadRepo")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	contents := []byte("not a directory")
	ioutil.WriteFile(filepath.Join(testDir, dbfactory.DoltDataDir), contents, 0644)

	ddb, err := LoadDoltDB(context.Background(), types.Format_7_18, LocalDirDoltDB)
	assert.Nil(t, ddb, "Should return nil when loading a non-directory data dir file")
	assert.Error(t, err, "Should see an error here")
}

func TestLDNoms(t *testing.T) {
	testDir, err := test.ChangeToTestDir("TestLoadRepo")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	committerName := "Bill Billerson"
	committerEmail := "bigbillieb@fake.horse"

	// Create an empty repo in a temp dir on the filesys
	{
		err := filesys.LocalFS.MkDirs(filepath.Join(testDir, dbfactory.DoltDataDir))

		if err != nil {
			t.Fatal("Failed to create noms directory")
		}

		ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, LocalDirDoltDB)
		err = ddb.WriteEmptyRepo(context.Background(), committerName, committerEmail)

		if err != nil {
			t.Fatal("Unexpected error creating empty repo", err)
		}
	}

	//read the empty repo back and add a new table.  Write the value, but don't commit
	var valHash hash.Hash
	var tbl *Table
	{
		ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, LocalDirDoltDB)
		cs, _ := NewCommitSpec("master", "")
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
		tbl, err = createTestTable(ddb.db, tSchema, rowData)

		if err != nil {
			t.Fatal("Failed to create test table with data")
		}

		root, err = root.PutTable(context.Background(), "test", tbl)
		assert.NoError(t, err)

		valHash, err = ddb.WriteRootValue(context.Background(), root)
		assert.NoError(t, err)
	}

	// reopen the db and commit the value.  Perform a couple checks for
	{
		ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, LocalDirDoltDB)
		meta, err := NewCommitMeta(committerName, committerEmail, "Sample data")
		if err != nil {
			t.Error("Failled to commit")
		}

		commit, err := ddb.Commit(context.Background(), valHash, ref.NewBranchRef("master"), meta)
		if err != nil {
			t.Error("Failled to commit")
		}

		numParents, err := commit.NumParents()
		assert.NoError(t, err)

		if numParents != 1 {
			t.Error("Unexpected ancestry")
		}

		root, err := commit.GetRootValue()
		assert.NoError(t, err)

		readTable, ok, err := root.GetTable(context.Background(), "test")
		assert.NoError(t, err)

		if !ok {
			t.Error("Could not retrieve test table")
		}

		has, err := readTable.HasTheSameSchema(tbl)
		assert.NoError(t, err)

		if !has {
			t.Error("Unexpected schema")
		}
	}
}
