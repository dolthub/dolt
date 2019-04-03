package doltdb

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/test"
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
	ddb := LoadDoltDB(InMemDoltDB)
	err := ddb.WriteEmptyRepo("Bill Billerson", "bigbillieb@fake.horse")

	if err != nil {
		t.Fatal("Unexpected error creating empty repo", err)
	}

	cs, _ := NewCommitSpec("HEAD", "master")
	commit, err := ddb.Resolve(cs)

	if err != nil {
		t.Fatal("Could not find commit")
	}

	cs2, _ := NewCommitSpec(commit.HashOf().String(), "")
	_, err = ddb.Resolve(cs2)

	if err != nil {
		t.Fatal("Failed to get commit by hash")
	}
}

func TestLoadNonExistentLocalFSRepo(t *testing.T) {
	_, err := test.ChangeToTestDir("TestLoadRepo")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	assert.Nil(t, LoadDoltDB(LocalDirDoltDB), "Should return nil when loading a non-existent data dir")
}

func TestLoadBadLocalFSRepo(t *testing.T) {
	testDir, err := test.ChangeToTestDir("TestLoadRepo")

	if err != nil {
		panic("Couldn't change the working directory to the test directory.")
	}

	contents := []byte("not a directory")
	ioutil.WriteFile(filepath.Join(testDir, DoltDataDir), contents, 0644)

	assert.Nil(t, LoadDoltDB(LocalDirDoltDB), "Should return nil when loading a non-directory data dir file")
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
		err := filesys.LocalFS.MkDirs(filepath.Join(testDir, DoltDataDir))

		if err != nil {
			t.Fatal("Failed to create noms directory")
		}

		ddb := LoadDoltDB(LocalDirDoltDB)
		err = ddb.WriteEmptyRepo(committerName, committerEmail)

		if err != nil {
			t.Fatal("Unexpected error creating empty repo", err)
		}
	}

	//read the empty repo back and add a new table.  Write the value, but don't commit
	var valHash hash.Hash
	var tbl *Table
	{
		ddb := LoadDoltDB(LocalDirDoltDB)
		cs, _ := NewCommitSpec("master", "")
		commit, err := ddb.Resolve(cs)

		if err != nil {
			t.Fatal("Couldn't find commit")
		}

		meta := commit.GetCommitMeta()

		if meta.Name != committerName || meta.Email != committerEmail {
			t.Error("Unexpected metadata")
		}

		root := commit.GetRootValue()

		if len(root.GetTableNames()) != 0 {
			t.Fatal("There should be no tables in empty db")
		}

		tSchema := createTestSchema()
		rowData, _ := createTestRowData(ddb.db, tSchema)
		tbl, err = createTestTable(ddb.db, tSchema, rowData)

		if err != nil {
			t.Fatal("Failed to create test table with data")
		}

		root = root.PutTable(ddb, "test", tbl)
		valHash, err = ddb.WriteRootValue(root)

		if err != nil {
			t.Fatal("Failed to write value")
		}
	}

	// reopen the db and commit the value.  Perform a couple checks for
	{
		ddb := LoadDoltDB(LocalDirDoltDB)
		meta, err := NewCommitMeta(committerName, committerEmail, "Sample data")
		if err != nil {
			t.Error("Failled to commit")
		}

		commit, err := ddb.Commit(valHash, "master", meta)
		if err != nil {
			t.Error("Failled to commit")
		}

		if commit.NumParents() != 1 {
			t.Error("Unexpected ancestry")
		}

		root := commit.GetRootValue()
		readTable, ok := root.GetTable("test")

		if !ok {
			t.Error("Could not retrieve test table")
		}

		if !readTable.HasTheSameSchema(tbl) {
			t.Error("Unexpected schema")
		}
	}
}
