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

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/utils/config"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	homePrefix = "home"
)

type MultiRepoTestSetup struct {
	envs    map[string]*env.DoltEnv
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
		envs:    make(map[string]*env.DoltEnv),
		Remotes: make(map[string]env.Remote),
		DoltDBs: make(map[string]*doltdb.DoltDB, 0),
		DbNames: make([]string, 0),
		Root:    dir,
		Home:    homeDir,
		DbPaths: make(map[string]string, 0),
		Errhand: errhand,
	}
}

func (mr *MultiRepoTestSetup) GetEnv(dbName string) *env.DoltEnv {
	return mr.envs[dbName]
}

func (mr *MultiRepoTestSetup) homeProv() (string, error) {
	return mr.Home, nil
}

func (mr *MultiRepoTestSetup) Close() {
	for _, db := range mr.DoltDBs {
		err := db.Close()
		if err != nil {
			mr.Errhand(err)
		}
	}
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
		config.UserNameKey:  name,
		config.UserEmailKey: email,
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

	mr.envs[dbName] = dEnv
	mr.DoltDBs[dbName] = ddb
	mr.DbNames = append(mr.DbNames, dbName)
	mr.DbPaths[dbName] = repo
}

func (mr *MultiRepoTestSetup) NewRemote(remoteName string) {
	remote := filepath.Join(mr.Root, remoteName)
	os.Mkdir(remote, os.ModePerm)
	remotePath := fmt.Sprintf("file:///%s", remote)

	rem := env.NewRemote(remoteName, remotePath, nil)

	for _, dEnv := range mr.envs {
		dEnv.RepoState.AddRemote(rem)
		dEnv.RepoState.Save(filesys.LocalFS)
	}

	mr.Remotes[remoteName] = rem
}

func (mr *MultiRepoTestSetup) NewBranch(dbName, branchName string) {
	dEnv := mr.envs[dbName]
	err := actions.CreateBranchWithStartPt(context.Background(), dEnv.DbData(), branchName, "head", false, nil)
	if err != nil {
		mr.Errhand(err)
	}
}

func (mr *MultiRepoTestSetup) CheckoutBranch(dbName, branchName string) {
	dEnv := mr.envs[dbName]
	cliCtx, _ := cmd.NewArgFreeCliContext(context.Background(), dEnv)
	_, sqlCtx, closeFunc, err := cliCtx.QueryEngine(context.Background())
	if err != nil {
		mr.Errhand(err)
	}
	defer closeFunc()
	err = dprocedures.MoveWorkingSetToBranch(sqlCtx, branchName, false, false)
	if err != nil {
		mr.Errhand(err)
	}
}

func (mr *MultiRepoTestSetup) CloneDB(fromRemote, dbName string) {
	ctx := context.Background()
	cloneDir := filepath.Join(mr.Root, dbName)

	r := mr.GetRemote(fromRemote)
	srcDB, err := r.GetRemoteDB(ctx, types.Format_Default, mr.envs[dbName])
	if err != nil {
		mr.Errhand(err)
	}

	dEnv := env.Load(context.Background(), mr.homeProv, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
	dEnv, err = actions.EnvForClone(ctx, srcDB.Format(), r, cloneDir, dEnv.FS, dEnv.Version, mr.homeProv)
	if err != nil {
		mr.Errhand(err)
	}

	err = actions.CloneRemote(ctx, srcDB, r.Name, "", false, -1, dEnv)
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

	ddb := dEnv.DoltDB

	mr.envs[dbName] = dEnv
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
	dEnv := mr.envs[dbName]
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

	t := datas.CommitterDate()
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		panic("couldn't get roots: " + err.Error())
	}
	pendingCommit, err := actions.GetCommitStaged(ctx, roots, ws, mergeParentCommits, dEnv.DbData().Ddb, actions.CommitStagedProps{
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

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		panic("couldn't get working set: " + err.Error())
	}

	commit, err := dEnv.DoltDB.CommitWithWorkingSet(
		ctx,
		headRef,
		ws.Ref(),
		pendingCommit,
		ws.WithStagedRoot(pendingCommit.Roots.Staged).WithWorkingRoot(pendingCommit.Roots.Working).ClearMerge(),
		prevHash,
		doltdb.TodoWorkingSetMeta(),
		nil,
	)
	if err != nil {
		panic("couldn't commit: " + err.Error())
	}
	return commit
}

func createTestDataTable(ctx context.Context, ddb *doltdb.DoltDB) (*table.InMemTable, schema.Schema) {
	rows, sch, err := dtestutils.RowsAndSchema()
	if err != nil {
		panic(err)
	}

	imt := table.NewInMemTable(sch)

	for _, r := range rows {
		err := imt.AppendRow(ctx, ddb.ValueReadWriter(), r)
		if err != nil {
			panic(err)
		}
	}

	return imt, sch
}

func (mr *MultiRepoTestSetup) CreateTable(ctx context.Context, dbName, tblName string) {
	dEnv := mr.envs[dbName]

	imt, sch := createTestDataTable(ctx, dEnv.DoltDB)
	rows := make([]row.Row, imt.NumRows())
	for i := 0; i < imt.NumRows(); i++ {
		r, err := imt.GetRow(i)
		if err != nil {
			mr.Errhand(fmt.Sprintf("Failed to create table: %s", err.Error()))
		}
		rows[i] = r
	}
	if err := createTestTable(dEnv, tblName, sch); err != nil {
		mr.Errhand(err)
	}
}

func (mr *MultiRepoTestSetup) StageAll(dbName string) {
	dEnv := mr.envs[dbName]

	ctx := context.Background()
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to get roots: %s", dbName))
	}

	roots, err = actions.StageAllTables(ctx, roots, true)
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
	dEnv := mr.envs[dbName]

	ap := cli.CreatePushArgParser()
	apr, err := ap.Parse([]string{remoteName, branchName})
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}
	targets, remote, err := env.NewPushOpts(ctx, apr, dEnv.RepoStateReader(), dEnv.DoltDB, false, false, false, false)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}

	remoteDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format(), mr.envs[dbName])
	if err != nil {
		mr.Errhand(actions.HandleInitRemoteStorageClientErr(remote.Name, remote.Url, err))
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to access .dolt directory: %s", err.Error()))
	}

	pushOptions := &env.PushOptions{
		Targets: targets,
		Remote:  remote,
		Rsr:     dEnv.RepoStateReader(),
		Rsw:     dEnv.RepoStateWriter(),
		SrcDb:   dEnv.DoltDB,
		DestDb:  remoteDB,
		TmpDir:  tmpDir,
	}
	_, err = actions.DoPush(ctx, pushOptions, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		mr.Errhand(fmt.Sprintf("Failed to push remote: %s", err.Error()))
	}
}

// createTestTable creates a new test table with the name, schema, and rows given.
func createTestTable(dEnv *env.DoltEnv, tableName string, sch schema.Schema) error {
	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()
	ns := dEnv.DoltDB.NodeStore()

	idx, err := durable.NewEmptyPrimaryIndex(ctx, vrw, ns, sch)
	if err != nil {
		return err
	}

	tbl, err := doltdb.NewTable(ctx, vrw, ns, sch, idx, nil, nil)
	if err != nil {
		return err
	}

	sch, err = tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", doltdb.ErrNomsIO, err)
	}

	newRoot, err := root.PutTable(ctx, doltdb.TableName{Name: tableName}, tbl)
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
