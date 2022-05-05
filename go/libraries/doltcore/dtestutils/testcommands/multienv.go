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
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
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
	Remotes map[string]env.Remote
	Errhand func(args ...interface{})
}

const (
	name          = "billy bob"
	email         = "bigbillieb@fake.horse"
	defaultBranch = "main"
)

// TODO this is not a proper builder, dbs need to be added before remotes
func NewMultiRepoTestSetup(errhand func(args ...interface{})) *MultiRepoTestSetup {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		errhand(err)
	}

	homeDir, err := os.MkdirTemp(dir, homePrefix)
	if err != nil {
		errhand(err)
	}

	return &MultiRepoTestSetup{
		MrEnv:   env.MultiRepoEnv{},
		Remotes: make(map[string]env.Remote),
		DoltDBs: make(map[string]*doltdb.DoltDB, 0),
		DbNames: make([]string, 0),
		Root:    dir,
		Home:    homeDir,
		DbPaths: make(map[string]string, 0),
		Errhand: errhand,
	}
}

func (mr *MultiRepoTestSetup) homeProv() (string, error) {
	return mr.Home, nil
}

func (mr *MultiRepoTestSetup) Cleanup(dbName string) {
	os.RemoveAll(mr.Root)
}

func (mr *MultiRepoTestSetup) NewDB(dbName string) {
	ctx := context.Background()

	repo := filepath.Join(mr.Root, dbName)
	os.Mkdir(repo, os.ModePerm)

	err := os.Chdir(repo)
	if err != nil {
		mr.Errhand(err)
	}

	// TODO sometimes tempfiles scrubber is racy with tempfolder deleter
	dEnv := env.Load(context.Background(), mr.homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
	if err != nil {
		mr.Errhand("Failed to initialize environment:" + err.Error())
	}
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		env.UserNameKey:  name,
		env.UserEmailKey: email,
	})
	err = dEnv.InitRepo(context.Background(), types.Format_Default, name, email, defaultBranch)
	if err != nil {
		mr.Errhand("Failed to initialize environment:" + err.Error())
	}

	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)
	if err != nil {
		mr.Errhand("Failed to initialize environment:" + err.Error())
	}

	dEnv = env.Load(context.Background(), mr.homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")

	mr.MrEnv.AddEnv(dbName, dEnv)
	mr.DoltDBs[dbName] = ddb
	mr.DbNames = append(mr.DbNames, dbName)
	mr.DbPaths[dbName] = repo
}

func (mr *MultiRepoTestSetup) NewRemote(remoteName string) {
	remote := filepath.Join(mr.Root, remoteName)
	os.Mkdir(remote, os.ModePerm)
	remotePath := fmt.Sprintf("file:///%s", remote)

	dEnv := mr.MrEnv.GetEnv(mr.DbNames[0])
	rem := env.NewRemote(remoteName, remotePath, nil, dEnv)

	mr.MrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		dEnv.RepoState.AddRemote(rem)
		dEnv.RepoState.Save(filesys.LocalFS)
		return false, nil
	})

	mr.Remotes[remoteName] = rem
}

func (mr *MultiRepoTestSetup) NewBranch(dbName, branchName string) {
	dEnv := mr.MrEnv.GetEnv(dbName)
	err := actions.CreateBranchWithStartPt(context.Background(), dEnv.DbData(), branchName, "head", false)
	if err != nil {
		mr.Errhand(err)
	}
}

func (mr *MultiRepoTestSetup) CheckoutBranch(dbName, branchName string) {
	dEnv := mr.MrEnv.GetEnv(dbName)
	err := actions.CheckoutBranch(context.Background(), dEnv, branchName, false)
	if err != nil {
		mr.Errhand(err)
	}
}

func (mr *MultiRepoTestSetup) CloneDB(fromRemote, dbName string) {
	ctx := context.Background()
	cloneDir := filepath.Join(mr.Root, dbName)

	r := mr.GetRemote(fromRemote)
	srcDB, err := r.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		mr.Errhand(err)
	}

	dEnv := env.Load(context.Background(), mr.homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
	dEnv, err = actions.EnvForClone(ctx, srcDB.Format(), r, cloneDir, dEnv.FS, dEnv.Version, mr.homeProv)
	if err != nil {
		mr.Errhand(err)
	}

	err = actions.CloneRemote(ctx, srcDB, r.Name, "", dEnv)
	if err != nil {
		mr.Errhand(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		mr.Errhand(err)
	}
	err = os.Chdir(cloneDir)
	if err != nil {
		mr.Errhand(err)
	}
	defer os.Chdir(wd)

	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, doltdb.LocalDirDoltDB, filesys.LocalFS)
	if err != nil {
		mr.Errhand("Failed to initialize environment:" + err.Error())
	}

	dEnv = env.Load(context.Background(), mr.homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")

	mr.MrEnv.AddEnv(dbName, dEnv)
	mr.DoltDBs[dbName] = ddb
	mr.DbNames = append(mr.DbNames, dbName)
	mr.DbPaths[dbName] = cloneDir
}

func (mr *MultiRepoTestSetup) GetRemote(remoteName string) env.Remote {
	rem, ok := mr.Remotes[remoteName]
	if !ok {
		mr.Errhand("remote not found")
	}
	return rem
}

func (mr *MultiRepoTestSetup) GetDB(dbName string) *doltdb.DoltDB {
	db, ok := mr.DoltDBs[dbName]
	if !ok {
		mr.Errhand("db not found")
	}
	return db
}

func (mr *MultiRepoTestSetup) CommitWithWorkingSet(dbName string) *doltdb.Commit {
	ctx := context.Background()
	dEnv := mr.MrEnv.GetEnv(dbName)
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

	t := datas.CommitNowFunc()
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		panic("couldn't get roots: " + err.Error())
	}
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
		doltdb.TodoWorkingSetMeta(),
	)
	if err != nil {
		panic("couldn't commit: " + err.Error())
	}
	return commit
}

func (mr *MultiRepoTestSetup) CreateTable(dbName, tblName string) {
	dEnv := mr.MrEnv.GetEnv(dbName)

	imt, sch := dtestutils.CreateTestDataTable(true)
	rows := make([]row.Row, imt.NumRows())
	for i := 0; i < imt.NumRows(); i++ {
		r, err := imt.GetRow(i)
		if err != nil {
			mr.Errhand(fmt.Sprintf("Failed to create table: %s", err.Error()))
		}
		rows[i] = r
	}
	createTestTable(dEnv, tblName, sch, mr.Errhand, rows...)
}

func (mr *MultiRepoTestSetup) StageAll(dbName string) {
	dEnv := mr.MrEnv.GetEnv(dbName)

	ctx := context.Background()
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to get roots: %s", dbName))
	}

	roots, err = actions.StageAllTables(ctx, roots, dEnv.Docs)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to stage tables: %s", dbName))
	}
	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to update roots: %s", dbName))
	}
}

func (mr *MultiRepoTestSetup) PushToRemote(dbName, remoteName, branchName string) {
	ctx := context.Background()
	dEnv := mr.MrEnv.GetEnv(dbName)

	ap := cli.CreatePushArgParser()
	apr, err := ap.Parse([]string{remoteName, branchName})
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}
	opts, err := env.NewPushOpts(ctx, apr, dEnv.RepoStateReader(), dEnv.DoltDB, false, false)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}
	err = actions.DoPush(ctx, dEnv.RepoStateReader(), dEnv.RepoStateWriter(), dEnv.DoltDB, dEnv.TempTableFilesDir(), opts, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}
}

// createTestTable creates a new test table with the name, schema, and rows given.
func createTestTable(dEnv *env.DoltEnv, tableName string, sch schema.Schema, errhand func(args ...interface{}), rs ...row.Row) {
	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()

	rowMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		errhand(err)
	}

	me := rowMap.Edit()
	for _, r := range rs {
		k, v := r.NomsMapKey(sch), r.NomsMapValue(sch)
		me.Set(k, v)
	}
	rowMap, err = me.Map(ctx)
	if err != nil {
		errhand(err)
	}

	tbl, err := doltdb.NewNomsTable(ctx, vrw, sch, rowMap, nil, nil)
	if err != nil {
		errhand(err)
	}
	tbl, err = editor.RebuildAllIndexes(ctx, tbl, editor.TestEditorOptions(vrw))
	if err != nil {
		errhand(err)
	}

	sch, err = tbl.GetSchema(ctx)
	if err != nil {
		errhand(err)
	}
	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		errhand(err)
	}
	indexes, err := tbl.GetIndexSet(ctx)
	if err != nil {
		errhand(err)
	}
	err = putTableToWorking(ctx, dEnv, sch, rows, indexes, tableName, nil)
	if err != nil {
		errhand(err)
	}
}

func putTableToWorking(ctx context.Context, dEnv *env.DoltEnv, sch schema.Schema, rows types.Map, indexData durable.IndexSet, tableName string, autoVal types.Value) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", doltdb.ErrNomsIO, err)
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	tbl, err := doltdb.NewNomsTable(ctx, vrw, sch, rows, indexData, autoVal)
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
