// Copyright 2019 Dolthub, Inc.
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

package dtestutils

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	TestHomeDir = "/user/bheni"
	WorkingDir  = "/user/bheni/datasets/states"
)

func testHomeDirFunc() (string, error) {
	return TestHomeDir, nil
}

func CreateTestEnv() *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"
	initialDirs := []string{TestHomeDir, WorkingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		env.UserNameKey:  name,
		env.UserEmailKey: email,
	})
	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email)

	if err != nil {
		panic("Failed to initialize environment:" + err.Error())
	}

	return dEnv
}

func CreateEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := CreateTestEnv()
	imt, sch := CreateTestDataTable(true)

	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()
	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(ctx, vrw, sch)

	_, _, err := table.PipeRows(ctx, rd, wr, false)
	require.NoError(t, err)
	err = rd.Close(ctx)
	require.NoError(t, err)
	err = wr.Close(ctx)
	require.NoError(t, err)

	ai := sch.Indexes().AllIndexes()
	sch = wr.GetSchema()
	sch.Indexes().Merge(ai...)

	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	require.NoError(t, err)
	empty, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	tbl, err := doltdb.NewTable(ctx, vrw, schVal, wr.GetMap(), empty, nil)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(ctx, tbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	sch, err = tbl.GetSchema(ctx)
	require.NoError(t, err)
	rows, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	indexes, err := tbl.GetIndexData(ctx)
	require.NoError(t, err)
	err = putTableToWorking(ctx, dEnv, sch, rows, indexes, TableName, nil)
	require.NoError(t, err)

	return dEnv
}

func putTableToWorking(ctx context.Context, dEnv *env.DoltEnv, sch schema.Schema, rows types.Map, indexData types.Map, tableName string, autoVal types.Value) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return env.ErrMarshallingSchema
	}

	tbl, err := doltdb.NewTable(ctx, vrw, schVal, rows, indexData, autoVal)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, tableName, tbl)
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	newRootHash, err := newRoot.HashOf()
	if err != nil {
		return err
	}
	if rootHash == newRootHash {
		return nil
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}

const (
	repoPrefix   = "repo_*"
	remotePrefix = "remote_*"
	homePrefix   = "home"
)

type MultiRepoTestSetup struct {
	MrEnv   env.MultiRepoEnv
	Remote  string
	DoltDBs map[string]*doltdb.DoltDB
	DbNames []string
	Root    string
	DbPaths map[string]string
	Home    string
}

const (
	name  = "billy bob"
	email = "bigbillieb@fake.horse"
)

func CreateTestMultiEnvWithRemote() *MultiRepoTestSetup {
	ctx := context.Background()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal(err)
	}

	homeDir, err := ioutil.TempDir(dir, homePrefix)
	if err != nil {
		log.Fatal(err)
	}
	homeProv := func() (string, error) {
		return homeDir, nil
	}

	remote, err := ioutil.TempDir(dir, remotePrefix)
	if err != nil {
		log.Fatal(err)
	}

	repoCnt := 2
	mrEnv := env.MultiRepoEnv{}
	dbs := make(map[string]*doltdb.DoltDB, repoCnt)
	dbNames := make([]string, repoCnt)
	dbPaths := make(map[string]string, repoCnt)
	for i := 0; i < repoCnt; i++ {
		repo, err := ioutil.TempDir(dir, repoPrefix)
		if err != nil {
			log.Fatal(err)
		}

		err = os.Chdir(repo)
		if err != nil {
			log.Fatal(err)
		}

		dbName := filepath.Base(repo)
		dbPaths[dbName] = repo
		repoPath := fmt.Sprintf("file://%s", repo)

		dEnv := env.Load(context.Background(), homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
		if err != nil {
			panic("Failed to initialize environment:" + err.Error())
		}
		cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
		cfg.SetStrings(map[string]string{
			env.UserNameKey:  name,
			env.UserEmailKey: email,
		})
		err = dEnv.InitRepo(context.Background(), types.Format_Default, name, email)
		if err != nil {
			panic("Failed to initialize environment:" + err.Error())
		}

		ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, filepath.Join(repoPath, dbfactory.DoltDir), filesys.LocalFS)
		if err != nil {
			panic("Failed to initialize environment:" + err.Error())
		}

		remotePath := fmt.Sprintf("file://%s", remote)
		rem := env.NewRemote("remote1", remotePath, nil, dEnv)
		dEnv.RepoState.AddRemote(rem)
		dEnv.RepoState.Save(filesys.LocalFS)
		dEnv = env.Load(context.Background(), homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")

		mrEnv.AddEnv(dbName, dEnv)
		dbs[dbName] = ddb
		dbNames[i] = dbName
	}

	return &MultiRepoTestSetup{
		MrEnv:   mrEnv,
		Remote:  fmt.Sprintf("file://%s", remote),
		DoltDBs: dbs,
		DbNames: dbNames,
		Root:    dir,
		Home:    homeDir,
		DbPaths: dbPaths,
	}
}

func (mr *MultiRepoTestSetup) CommitWithWorkingSet(dbName string) *doltdb.Commit {
	ctx := context.Background()
	dEnv, ok := mr.MrEnv[dbName]
	if !ok {
		panic("database not found")
	}
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		panic("couldn't get working set: " + err.Error())
	}

	prevHash, err := ws.HashOf()
	if err != nil {
		panic("couldn't get working set: " + err.Error())
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	t := doltdb.CommitNowFunc()
	roots, err := dEnv.Roots(ctx)

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    "auto commit",
		Date:       t,
		AllowEmpty: true,
		Force:      false,
		Name:       name,
		Email:      email,
	})
	if err != nil {
		panic("pending commit error: " + err.Error())
	}

	commit, err := dEnv.DoltDB.CommitWithWorkingSet(
		ctx,
		dEnv.RepoStateReader().CWBHeadRef(),
		ws.Ref(),
		pendingCommit,
		ws.WithStagedRoot(pendingCommit.Roots.Staged).WithWorkingRoot(pendingCommit.Roots.Working).ClearMerge(),
		prevHash,
		dEnv.NewWorkingSetMeta(fmt.Sprintf("Updated by test")),
	)
	if err != nil {
		panic("couldn't commit: " + err.Error())
	}
	return commit
}

const (
	PeopleIdTag = iota + 200
	FirstNameTag
	LastNameTag
	emptyTag
	RatingTag
	UuidTag
	NumEpisodesTag
	firstUnusedTag // keep at end
)

const (
	HomerId = iota
	MargeId
	BartId
	LisaId
	MoeId
	BarneyId
)

var PeopleTestSchema = createPeopleTestSchema()
var untypedPeopleSch, _ = untyped.UntypeUnkeySchema(PeopleTestSchema)
var PeopleTableName = "people"

func createPeopleTestSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", PeopleIdTag, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", AgeTag, types.IntKind, false),
		schema.NewColumn("rating", RatingTag, types.FloatKind, false),
		schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
		schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

func NewPeopleRow(id int, first, last string, isMarried bool, age int, rating float64) row.Row {
	vals := row.TaggedValues{
		PeopleIdTag:  types.Int(id),
		FirstNameTag: types.String(first),
		LastNameTag:  types.String(last),
		IsMarriedTag: types.Bool(isMarried),
		AgeTag:       types.Int(age),
		RatingTag:    types.Float(rating),
	}

	r, err := row.New(types.Format_Default, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

// Most rows don't have these optional fields set, as they aren't needed for basic testing
func NewPeopleRowWithOptionalFields(id int, first, last string, isMarried bool, age int, rating float64, uid uuid.UUID, numEpisodes uint64) row.Row {
	vals := row.TaggedValues{
		PeopleIdTag:    types.Int(id),
		FirstNameTag:   types.String(first),
		LastNameTag:    types.String(last),
		IsMarriedTag:   types.Bool(isMarried),
		AgeTag:         types.Int(age),
		RatingTag:      types.Float(rating),
		UuidTag:        types.UUID(uid),
		NumEpisodesTag: types.Uint(numEpisodes),
	}

	r, err := row.New(types.Format_Default, PeopleTestSchema, vals)

	if err != nil {
		panic(err)
	}

	return r
}

var Homer = NewPeopleRow(HomerId, "Homer", "Simpson", true, 40, 8.5)
var Marge = NewPeopleRowWithOptionalFields(MargeId, "Marge", "Simpson", true, 38, 8, uuid.MustParse("00000000-0000-0000-0000-000000000001"), 111)
var Bart = NewPeopleRowWithOptionalFields(BartId, "Bart", "Simpson", false, 10, 9, uuid.MustParse("00000000-0000-0000-0000-000000000002"), 222)
var Lisa = NewPeopleRowWithOptionalFields(LisaId, "Lisa", "Simpson", false, 8, 10, uuid.MustParse("00000000-0000-0000-0000-000000000003"), 333)
var Moe = NewPeopleRowWithOptionalFields(MoeId, "Moe", "Szyslak", false, 48, 6.5, uuid.MustParse("00000000-0000-0000-0000-000000000004"), 444)
var Barney = NewPeopleRowWithOptionalFields(BarneyId, "Barney", "Gumble", false, 40, 4, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 555)
var AllPeopleRows = []row.Row{Homer, Marge, Bart, Lisa, Moe, Barney}

func (mr *MultiRepoTestSetup) CreateTable(t *testing.T, dbName, tblName string) {
	dEnv, ok := mr.MrEnv[dbName]
	if !ok {
		t.Fatalf("Failed to find db: %s", dbName)
	}

	CreateTestTable(t, dEnv, tblName, PeopleTestSchema, AllPeopleRows...)
}

func (mr *MultiRepoTestSetup) AddAll(t *testing.T, dbName string) {
	dEnv, ok := mr.MrEnv[dbName]
	if !ok {
		t.Fatalf("Failed to find db: %s", dbName)
	}
	ctx := context.Background()
	roots, err := dEnv.Roots(ctx)
	if !ok {
		t.Fatalf("Failed to get roots: %s", dbName)
	}

	roots, err = actions.StageAllTables(ctx, roots, dEnv.Docs)
	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		t.Fatalf("Failed to update roots: %s", dbName)
	}
}

func (mr *MultiRepoTestSetup) PushToRemote(t *testing.T, dbName string) {
	ctx := context.Background()
	dEnv, ok := mr.MrEnv[dbName]
	if !ok {
		t.Fatalf("Failed to find db: %s", dbName)
	}

	ap := cli.CreatePushArgParser()
	apr, err := ap.Parse([]string{"remote1", "master"})
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
	opts, err := env.NewParseOpts(ctx, apr, dEnv.RepoStateReader(), dEnv.DoltDB, false, false)
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
	err = actions.DoPush(ctx, dEnv.RepoStateReader(), dEnv.RepoStateWriter(), dEnv.DoltDB, dEnv.TempTableFilesDir(), opts, actions.DefaultRunProgFuncs, actions.DefaultStopProgFuncs)
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
}
