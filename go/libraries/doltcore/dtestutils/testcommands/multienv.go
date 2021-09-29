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

package testcommands

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

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
	name          = "billy bob"
	email         = "bigbillieb@fake.horse"
	defaultBranch = "main"
)

func CreateMultiEnvWithRemote() *MultiRepoTestSetup {
	ctx := context.Background()
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	defer os.Chdir(cwd)

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

		// TODO sometimes tempfiles scrubber is racy with tempfolder deleter
		dEnv := env.Load(context.Background(), homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
		if err != nil {
			panic("Failed to initialize environment:" + err.Error())
		}
		cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
		cfg.SetStrings(map[string]string{
			env.UserNameKey:  name,
			env.UserEmailKey: email,
		})
		err = dEnv.InitRepo(context.Background(), types.Format_Default, name, email, defaultBranch)
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

func (mr *MultiRepoTestSetup) CreateTable(t *testing.T, dbName, tblName string) {
	dEnv, ok := mr.MrEnv[dbName]
	if !ok {
		t.Fatalf("Failed to find db: %s", dbName)
	}

	imt, sch := dtestutils.CreateTestDataTable(true)
	rows := make([]row.Row, imt.NumRows())
	for i := 0; i < imt.NumRows(); i++ {
		r, err := imt.GetRow(i)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err.Error())
		}
		rows[i] = r
	}
	dtestutils.CreateTestTable(t, dEnv, tblName, sch, rows...)
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
	apr, err := ap.Parse([]string{"remote1", defaultBranch})
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
	opts, err := env.NewParseOpts(ctx, apr, dEnv.RepoStateReader(), dEnv.DoltDB, false, false)
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
	err = actions.DoPush(ctx, dEnv.RepoStateReader(), dEnv.RepoStateWriter(), dEnv.DoltDB, dEnv.TempTableFilesDir(), opts, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		t.Fatalf("Failed to push remote: %s", err.Error())
	}
}
