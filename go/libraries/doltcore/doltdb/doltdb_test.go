package doltdb

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/test"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"testing"
)

const (
	idTag        = 0
	firstTag     = 1
	lastTag      = 2
	isMarriedTag = 3
	ageTag       = 4
	emptyTag     = 5
)

func createTestSchema() schema.Schema {
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.UintKind, false),
		schema.NewColumn("empty", emptyTag, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)

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

	cs, _ := NewCommitSpec("HEAD", "master")
	commit, err := ddb.Resolve(context.Background(), cs)

	if err != nil {
		t.Fatal("Could not find commit")
	}

	cs2, _ := NewCommitSpec(commit.HashOf().String(), "")
	_, err = ddb.Resolve(context.Background(), cs2)

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
		commit, err := ddb.Resolve(context.Background(), cs)

		if err != nil {
			t.Fatal("Couldn't find commit")
		}

		meta := commit.GetCommitMeta()

		if meta.Name != committerName || meta.Email != committerEmail {
			t.Error("Unexpected metadata")
		}

		root := commit.GetRootValue()

		if len(root.GetTableNames(context.Background())) != 0 {
			t.Fatal("There should be no tables in empty db")
		}

		tSchema := createTestSchema()
		rowData, _ := createTestRowData(ddb.db, tSchema)
		tbl, err = createTestTable(ddb.db, tSchema, rowData)

		if err != nil {
			t.Fatal("Failed to create test table with data")
		}

		root = root.PutTable(context.Background(), ddb, "test", tbl)
		valHash, err = ddb.WriteRootValue(context.Background(), root)

		if err != nil {
			t.Fatal("Failed to write value")
		}
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

		if commit.NumParents() != 1 {
			t.Error("Unexpected ancestry")
		}

		root := commit.GetRootValue()
		readTable, ok := root.GetTable(context.Background(), "test")

		if !ok {
			t.Error("Could not retrieve test table")
		}

		if !readTable.HasTheSameSchema(tbl) {
			t.Error("Unexpected schema")
		}
	}
}
