// Copyright 2019-2020 Dolthub, Inc.
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

package env

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	DefaultInitBranch = "main"

	DefaultLoginUrl = "https://dolthub.com/settings/credentials"

	DefaultMetricsHost = "eventsapi.dolthub.com"
	DefaultMetricsPort = "443"

	DefaultRemotesApiHost = "doltremoteapi.dolthub.com"
	DefaultRemotesApiPort = "443"

	tempTablesDir = "temptf"
)

var zeroHashStr = (hash.Hash{}).String()

var ErrPreexistingDoltDir = errors.New(".dolt dir already exists")
var ErrStateUpdate = errors.New("error updating local data repo state")
var ErrMarshallingSchema = errors.New("error marshalling schema")
var ErrInvalidCredsFile = errors.New("invalid creds file")
var ErrRemoteAlreadyExists = errors.New("remote already exists")
var ErrInvalidRemoteURL = errors.New("remote URL invalid")
var ErrRemoteNotFound = errors.New("remote not found")
var ErrInvalidRemoteName = errors.New("remote name invalid")
var ErrBackupAlreadyExists = errors.New("backup already exists")
var ErrInvalidBackupURL = errors.New("backup URL invalid")
var ErrBackupNotFound = errors.New("backup not found")
var ErrInvalidBackupName = errors.New("backup name invalid")
var ErrFailedToDeleteBackup = errors.New("failed to delete backup")
var ErrFailedToReadFromDb = errors.New("failed to read from db")
var ErrFailedToDeleteRemote = errors.New("failed to delete remote")
var ErrFailedToWriteRepoState = errors.New("failed to write repo state")
var ErrRemoteAddressConflict = errors.New("address conflict with a remote")

// DoltEnv holds the state of the current environment used by the cli.
type DoltEnv struct {
	Version string

	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

	Docs        doltdocs.Docs
	DocsLoadErr error

	DoltDB      *doltdb.DoltDB
	DBLoadError error

	FS     filesys.Filesys
	urlStr string
	hdp    HomeDirProvider
}

// Load loads the DoltEnv for the current directory of the cli
func Load(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, urlStr, version string) *DoltEnv {
	config, cfgErr := LoadDoltCliConfig(hdp, fs)
	repoState, rsErr := LoadRepoState(fs)

	docs, docsErr := doltdocs.LoadDocs(fs)
	ddb, dbLoadErr := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr, fs)

	dEnv := &DoltEnv{
		Version:     version,
		Config:      config,
		CfgLoadErr:  cfgErr,
		RepoState:   repoState,
		RSLoadErr:   rsErr,
		Docs:        docs,
		DocsLoadErr: docsErr,
		DoltDB:      ddb,
		DBLoadError: dbLoadErr,
		FS:          fs,
		urlStr:      urlStr,
		hdp:         hdp,
	}
	if dEnv.RepoState != nil {
		remotes := make(map[string]Remote, len(dEnv.RepoState.Remotes))
		for n, r := range dEnv.RepoState.Remotes {
			r.dialer = dEnv
			remotes[n] = r
		}
		dEnv.RepoState.Remotes = remotes

		backups := make(map[string]Remote, len(dEnv.RepoState.Backups))
		for n, r := range dEnv.RepoState.Backups {
			r.dialer = dEnv
			backups[n] = r
		}
		dEnv.RepoState.Backups = backups
	}

	if dbLoadErr == nil && dEnv.HasDoltDir() {
		if !dEnv.HasDoltTempTableDir() {
			err := dEnv.FS.MkDirs(dEnv.TempTableFilesDir())
			dEnv.DBLoadError = err
		} else {
			// fire and forget cleanup routine.  Will delete as many old temp files as it can during the main commands execution.
			// The process will not wait for this to finish so this may not always complete.
			go func() {
				// TODO dEnv.HasDoltTempTableDir() true but dEnv.TempTableFileDir() panics
				tmpTableDir, err := dEnv.FS.Abs(filepath.Join(dEnv.urlStr, dbfactory.DoltDir, tempTablesDir))
				if err != nil {
					return
				}
				_ = fs.Iter(tmpTableDir, true, func(path string, size int64, isDir bool) (stop bool) {
					if !isDir {
						lm, exists := fs.LastModified(path)

						if exists && time.Now().Sub(lm) > (time.Hour*24) {
							_ = fs.DeleteFile(path)
						}
					}

					return false
				})
			}()
		}
	}

	if rsErr == nil && dbLoadErr == nil {
		// If the working set isn't present in the DB, create it from the repo state. This step can be removed post 1.0.
		_, err := dEnv.WorkingSet(ctx)
		if err == doltdb.ErrWorkingSetNotFound {
			err := dEnv.initWorkingSetFromRepoState(ctx)
			if err != nil {
				dEnv.RSLoadErr = err
			}
		} else if err != nil {
			dEnv.RSLoadErr = err
		}
	}

	return dEnv
}

func GetDefaultInitBranch(cfg config.ReadableConfig) string {
	return GetStringOrDefault(cfg, InitBranchName, DefaultInitBranch)
}

// Valid returns whether this environment has been properly initialized. This is useful because although every command
// gets a DoltEnv, not all of them require it, and we allow invalid dolt envs to be passed around for this reason.
func (dEnv *DoltEnv) Valid() bool {
	return dEnv.CfgLoadErr == nil && dEnv.DBLoadError == nil && dEnv.HasDoltDir() && dEnv.HasDoltDataDir()
}

// initWorkingSetFromRepoState sets the working set for the env's head to mirror the contents of the repo state file.
// This is only necessary to migrate repos written before this method was introduced, and can be removed after 1.0
func (dEnv *DoltEnv) initWorkingSetFromRepoState(ctx context.Context) error {
	headRef := dEnv.RepoStateReader().CWBHeadRef()
	wsRef, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return err
	}

	stagedRoot := headRoot
	if len(dEnv.RepoState.staged) != 0 && dEnv.RepoState.staged != zeroHashStr {
		stagedHash, ok := hash.MaybeParse(dEnv.RepoState.staged)
		if !ok {
			return fmt.Errorf("Corrupt repo, invalid staged hash %s", stagedHash)
		}

		stagedRoot, err = dEnv.DoltDB.ReadRootValue(ctx, stagedHash)
		if err != nil {
			return err
		}
	}

	workingRoot := stagedRoot
	if len(dEnv.RepoState.working) != 0 && dEnv.RepoState.working != zeroHashStr {
		workingHash, ok := hash.MaybeParse(dEnv.RepoState.working)
		if !ok {
			return fmt.Errorf("Corrupt repo, invalid working hash %s", workingHash)
		}

		workingRoot, err = dEnv.DoltDB.ReadRootValue(ctx, workingHash)
		if err != nil {
			return err
		}
	}

	mergeState, err := mergeStateToMergeState(ctx, dEnv.RepoState.merge, dEnv.DoltDB)
	if err != nil {
		return err
	}

	ws := doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot).WithMergeState(mergeState)
	return dEnv.UpdateWorkingSet(ctx, ws)
}

func mergeStateToMergeState(ctx context.Context, mergeState *mergeState, db *doltdb.DoltDB) (*doltdb.MergeState, error) {
	if mergeState == nil {
		return nil, nil
	}

	cs, err := doltdb.NewCommitSpec(mergeState.Commit)
	if err != nil {
		panic("Corrupted repostate. Active merge state is not valid.")
	}

	commit, err := db.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, err
	}

	pmwh := hash.Parse(mergeState.PreMergeWorking)
	pmwr, err := db.ReadRootValue(ctx, pmwh)
	if err != nil {
		return nil, err
	}

	return doltdb.MergeStateFromCommitAndWorking(commit, pmwr), nil
}

// HasDoltDir returns true if the .dolt directory exists and is a valid directory
func (dEnv *DoltEnv) HasDoltDir() bool {
	return dEnv.hasDoltDir("./")
}

func (dEnv *DoltEnv) HasDoltDataDir() bool {
	exists, isDir := dEnv.FS.Exists(dbfactory.DoltDataDir)
	return exists && isDir
}

func (dEnv *DoltEnv) HasDoltTempTableDir() bool {
	ex, _ := dEnv.FS.Exists(dEnv.TempTableFilesDir())

	return ex
}

func mustAbs(dEnv *DoltEnv, path ...string) string {
	absPath, err := dEnv.FS.Abs(filepath.Join(path...))

	if err != nil {
		panic(err)
	}

	return absPath
}

// GetDoltDir returns the path to the .dolt directory
func (dEnv *DoltEnv) GetDoltDir() string {
	if !dEnv.HasDoltDataDir() {
		panic("No dolt dir")
	}

	return mustAbs(dEnv, dbfactory.DoltDir)
}

func (dEnv *DoltEnv) hasDoltDir(path string) bool {
	exists, isDir := dEnv.FS.Exists(mustAbs(dEnv, dbfactory.DoltDir))
	return exists && isDir
}

// HasLocalConfig returns true if a repository local config file
func (dEnv *DoltEnv) HasLocalConfig() bool {
	_, ok := dEnv.Config.GetConfig(LocalConfig)

	return ok
}

func (dEnv *DoltEnv) bestEffortDeleteAll(dir string) {
	fileToIsDir := make(map[string]bool)
	dEnv.FS.Iter(dir, false, func(path string, size int64, isDir bool) (stop bool) {
		fileToIsDir[path] = isDir
		return false
	})

	for path, isDir := range fileToIsDir {
		if isDir {
			dEnv.FS.Delete(path, true)
		} else {
			dEnv.FS.DeleteFile(path)
		}
	}
}

// InitRepo takes an empty directory and initializes it with a .dolt directory containing repo state, uncommitted license and readme, and creates a noms
// database with dolt structure.
func (dEnv *DoltEnv) InitRepo(ctx context.Context, nbf *types.NomsBinFormat, name, email, branchName string) error { // should remove name and email args
	return dEnv.InitRepoWithTime(ctx, nbf, name, email, branchName, doltdb.CommitNowFunc())
}

func (dEnv *DoltEnv) InitRepoWithTime(ctx context.Context, nbf *types.NomsBinFormat, name, email, branchName string, t time.Time) error { // should remove name and email args
	doltDir, err := dEnv.createDirectories(".")

	if err != nil {
		return err
	}

	err = dEnv.configureRepo(doltDir)

	if err == nil {
		err = dEnv.InitDBAndRepoState(ctx, nbf, name, email, branchName, t)
	}

	if err != nil {
		dEnv.bestEffortDeleteAll(dbfactory.DoltDir)
	}

	return err
}

func (dEnv *DoltEnv) InitRepoWithNoData(ctx context.Context, nbf *types.NomsBinFormat) error {
	doltDir, err := dEnv.createDirectories(".")

	if err != nil {
		return err
	}

	err = dEnv.configureRepo(doltDir)

	if err != nil {
		dEnv.bestEffortDeleteAll(dbfactory.DoltDir)
		return err
	}

	dEnv.DoltDB, err = doltdb.LoadDoltDB(ctx, nbf, dEnv.urlStr, filesys.LocalFS)

	return err
}

func (dEnv *DoltEnv) createDirectories(dir string) (string, error) {
	absPath, err := dEnv.FS.Abs(dir)

	if err != nil {
		return "", err
	}

	exists, isDir := dEnv.FS.Exists(absPath)

	if !exists {
		return "", fmt.Errorf("'%s' does not exist so could not create '%s", absPath, dbfactory.DoltDataDir)
	} else if !isDir {
		return "", fmt.Errorf("'%s' exists but it's a file not a directory", absPath)
	}

	if dEnv.hasDoltDir(dir) {
		return "", ErrPreexistingDoltDir
	}

	absDataDir := filepath.Join(absPath, dbfactory.DoltDataDir)
	err = dEnv.FS.MkDirs(absDataDir)

	if err != nil {
		return "", fmt.Errorf("unable to make directory '%s', cause: %s", absDataDir, err.Error())
	}

	err = dEnv.FS.MkDirs(dEnv.TempTableFilesDir())

	if err != nil {
		return "", fmt.Errorf("unable to make directory '%s', cause: %s", dEnv.TempTableFilesDir(), err.Error())
	}

	return filepath.Join(absPath, dbfactory.DoltDir), nil
}

func (dEnv *DoltEnv) configureRepo(doltDir string) error {
	err := dEnv.Config.CreateLocalConfig(map[string]string{})

	if err != nil {
		return fmt.Errorf("failed creating file %s", getLocalConfigPath())
	}

	return nil
}

// Inits the dolt DB of this environment with an empty commit at the time given and writes default docs to disk.
// Writes new repo state with a main branch and current root hash.
func (dEnv *DoltEnv) InitDBAndRepoState(ctx context.Context, nbf *types.NomsBinFormat, name, email, branchName string, t time.Time) error {
	err := dEnv.InitDBWithTime(ctx, nbf, name, email, branchName, t)
	if err != nil {
		return err
	}

	return dEnv.InitializeRepoState(ctx, branchName)
}

// Inits the dolt DB of this environment with an empty commit at the time given and writes default docs to disk.
// Does not update repo state.
func (dEnv *DoltEnv) InitDBWithTime(ctx context.Context, nbf *types.NomsBinFormat, name, email, branchName string, t time.Time) error {
	var err error
	dEnv.DoltDB, err = doltdb.LoadDoltDB(ctx, nbf, dEnv.urlStr, dEnv.FS)
	if err != nil {
		return err
	}
	// TODO(andy): this would be a better place to close the database

	err = dEnv.DoltDB.WriteEmptyRepoWithCommitTime(ctx, branchName, name, email, t)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	return nil
}

// InitializeRepoState writes a default repo state to disk, consisting of a main branch and current root hash value.
func (dEnv *DoltEnv) InitializeRepoState(ctx context.Context, branchName string) error {
	commit, err := dEnv.DoltDB.ResolveCommitRef(ctx, ref.NewBranchRef(branchName))
	if err != nil {
		return err
	}

	root, err := commit.GetRootValue()
	if err != nil {
		return err
	}

	dEnv.RepoState, err = CreateRepoState(dEnv.FS, branchName)
	if err != nil {
		return ErrStateUpdate
	}

	// TODO: combine into one update
	err = dEnv.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return err
	}

	err = dEnv.UpdateStagedRoot(ctx, root)
	if err != nil {
		return err
	}

	dEnv.RSLoadErr = nil
	return nil
}

type RootsProvider interface {
	GetRoots(ctx context.Context) (doltdb.Roots, error)
}

// Roots returns the roots for this environment
func (dEnv *DoltEnv) Roots(ctx context.Context) (doltdb.Roots, error) {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return doltdb.Roots{
		Head:    headRoot,
		Working: ws.WorkingRoot(),
		Staged:  ws.StagedRoot(),
	}, nil
}

// RecoveryRoots returns the roots for this environment in the case that the
// currently checked out branch has been deleted or HEAD has been updated in a
// non-principled way to point to a branch that does not exist. This is used by
// `dolt checkout`, in particular, to go forward with a `dolt checkout` of an
// existing branch in the degraded state where the current branch was deleted.
func (dEnv *DoltEnv) RecoveryRoots(ctx context.Context) (doltdb.Roots, error) {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err == doltdb.ErrBranchNotFound {
		headRoot = ws.StagedRoot()
		err = nil
	}
	if err != nil {
		return doltdb.Roots{}, err
	}

	return doltdb.Roots{
		Head:    headRoot,
		Working: ws.WorkingRoot(),
		Staged:  ws.StagedRoot(),
	}, nil
}

// UpdateRoots updates the working and staged roots for this environment
func (dEnv *DoltEnv) UpdateRoots(ctx context.Context, roots doltdb.Roots) error {
	ws, err := dEnv.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating roots
		wsRef, err := ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	}

	return dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged))
}

// WorkingRoot returns the working root for the current working branch
func (dEnv *DoltEnv) WorkingRoot(ctx context.Context) (*doltdb.RootValue, error) {
	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return nil, err
	}

	return workingSet.WorkingRoot(), nil
}

func (dEnv *DoltEnv) WorkingSet(ctx context.Context) (*doltdb.WorkingSet, error) {
	return WorkingSet(ctx, dEnv.DoltDB, dEnv.RepoStateReader())
}

func WorkingSet(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (*doltdb.WorkingSet, error) {
	workingSetRef, err := ref.WorkingSetRefForHead(rsr.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	workingSet, err := ddb.ResolveWorkingSet(ctx, workingSetRef)
	if err != nil {
		return nil, err
	}

	return workingSet, nil
}

// UpdateWorkingRoot updates the working root for the current working branch to the root value given.
// This method can fail if another client updates the working root at the same time.
func (dEnv *DoltEnv) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := dEnv.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(newRoot).WithStagedRoot(newRoot)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	// TODO: add actual trace logging here
	// logrus.Infof("Updating working root to %s", newRoot.DebugString(context.Background(), true))

	return dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithWorkingRoot(newRoot), h, dEnv.workingSetMeta())
}

// UpdateWorkingSet updates the working set for the current working branch to the value given.
// This method can fail if another client updates the working set at the same time.
func (dEnv *DoltEnv) UpdateWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	h, err := ws.HashOf()
	if err != nil {
		return err
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, ws.Ref(), ws, h, dEnv.workingSetMeta())
}

type repoStateReader struct {
	*DoltEnv
}

func (r *repoStateReader) CWBHeadRef() ref.DoltRef {
	return r.RepoState.CWBHeadRef()
}

func (r *repoStateReader) CWBHeadSpec() *doltdb.CommitSpec {
	return r.RepoState.CWBHeadSpec()
}

func (dEnv *DoltEnv) RepoStateReader() RepoStateReader {
	return &repoStateReader{dEnv}
}

type repoStateWriter struct {
	*DoltEnv
}

func (r *repoStateWriter) SetCWBHeadRef(ctx context.Context, marshalableRef ref.MarshalableRef) error {
	r.RepoState.Head = marshalableRef
	err := r.RepoState.Save(r.FS)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (r *repoStateWriter) AddRemote(name string, url string, fetchSpecs []string, params map[string]string) error {
	return r.DoltEnv.AddRemote(name, url, fetchSpecs, params)
}

func (r *repoStateWriter) AddBackup(name string, url string, fetchSpecs []string, params map[string]string) error {
	return r.DoltEnv.AddBackup(name, url, fetchSpecs, params)
}

func (r *repoStateWriter) RemoveRemote(ctx context.Context, name string) error {
	return r.DoltEnv.RemoveRemote(ctx, name)
}

func (r *repoStateWriter) RemoveBackup(ctx context.Context, name string) error {
	return r.DoltEnv.RemoveBackup(ctx, name)
}

func (dEnv *DoltEnv) RepoStateWriter() RepoStateWriter {
	return &repoStateWriter{dEnv}
}

type docsReadWriter struct {
	FS filesys.Filesys
}

// GetDocsOnDisk reads the filesystem and returns all docs.
func (d *docsReadWriter) GetDocsOnDisk(docNames ...string) (doltdocs.Docs, error) {
	if docNames != nil {
		ret := make(doltdocs.Docs, len(docNames))

		for i, name := range docNames {
			doc, err := doltdocs.GetDoc(d.FS, name)
			if err != nil {
				return nil, err
			}
			ret[i] = doc
		}

		return ret, nil
	}

	return doltdocs.GetSupportedDocs(d.FS)
}

// WriteDocsToDisk creates or updates the dolt_docs table with docs.
func (d *docsReadWriter) WriteDocsToDisk(docs doltdocs.Docs) error {
	return docs.Save(d.FS)
}

func (dEnv *DoltEnv) DocsReadWriter() DocsReadWriter {
	return &docsReadWriter{dEnv.FS}
}

func (dEnv *DoltEnv) HeadRoot(ctx context.Context) (*doltdb.RootValue, error) {
	commit, err := dEnv.DoltDB.ResolveCommitRef(ctx, dEnv.RepoState.CWBHeadRef())

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

func (dEnv *DoltEnv) DbData() DbData {
	return DbData{
		Ddb: dEnv.DoltDB,
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
		Drw: dEnv.DocsReadWriter(),
	}
}

// StagedRoot returns the staged root value in the current working set
func (dEnv *DoltEnv) StagedRoot(ctx context.Context) (*doltdb.RootValue, error) {
	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return nil, err
	}

	return workingSet.StagedRoot(), nil
}

// UpdateStagedRoot updates the staged root for the current working branch. This can fail if multiple clients attempt
// to update at the same time.
func (dEnv *DoltEnv) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := dEnv.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(newRoot).WithStagedRoot(newRoot)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithStagedRoot(newRoot), h, dEnv.workingSetMeta())
}

func (dEnv *DoltEnv) AbortMerge(ctx context.Context) error {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	h, err := ws.HashOf()
	if err != nil {
		return err
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, ws.Ref(), ws.AbortMerge(), h, dEnv.workingSetMeta())
}

func (dEnv *DoltEnv) workingSetMeta() *doltdb.WorkingSetMeta {
	return dEnv.NewWorkingSetMeta("updated from dolt environment")
}

func (dEnv *DoltEnv) NewWorkingSetMeta(message string) *doltdb.WorkingSetMeta {
	return &doltdb.WorkingSetMeta{
		User:        dEnv.Config.GetStringOrDefault(UserNameKey, ""),
		Email:       dEnv.Config.GetStringOrDefault(UserEmailKey, ""),
		Timestamp:   uint64(time.Now().Unix()),
		Description: message,
	}
}

func (dEnv *DoltEnv) ClearMerge(ctx context.Context) error {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	h, err := ws.HashOf()
	if err != nil {
		return err
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, ws.Ref(), ws.ClearMerge(), h, dEnv.workingSetMeta())
}

func (dEnv *DoltEnv) StartMerge(ctx context.Context, commit *doltdb.Commit) error {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	h, err := ws.HashOf()
	if err != nil {
		return err
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, ws.Ref(), ws.StartMerge(commit), h, dEnv.workingSetMeta())
}

func (dEnv *DoltEnv) IsMergeActive(ctx context.Context) (bool, error) {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return false, err
	}

	return ws.MergeActive(), nil
}

func (dEnv *DoltEnv) GetTablesWithConflicts(ctx context.Context) ([]string, error) {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, err
	}

	return root.TablesInConflict(ctx)
}

func (dEnv *DoltEnv) CredsDir() (string, error) {
	return getCredsDir(dEnv.hdp)
}

func (dEnv *DoltEnv) UserRPCCreds() (creds.DoltCreds, bool, error) {
	kid, err := dEnv.Config.GetString(UserCreds)

	if err == nil && kid != "" {
		dir, err := dEnv.CredsDir()

		if err != nil {
			// not sure why you wouldn't be able to get the creds dir.
			panic(err)
		}

		c, err := creds.JWKCredsReadFromFile(dEnv.FS, filepath.Join(dir, kid+".jwk"))
		return c, c.IsPrivKeyValid() && c.IsPubKeyValid(), err
	}

	return creds.EmptyCreds, false, nil
}

func (dEnv *DoltEnv) getRPCCreds() (credentials.PerRPCCredentials, error) {
	dCreds, valid, err := dEnv.UserRPCCreds()
	if err != nil {
		return nil, ErrInvalidCredsFile
	}
	if !valid {
		return nil, nil
	}
	return dCreds, nil
}

func (dEnv *DoltEnv) getUserAgentString() string {
	tokens := []string{
		"dolt_cli",
		dEnv.Version,
		runtime.GOOS,
		runtime.GOARCH,
	}

	for i, t := range tokens {
		tokens[i] = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return '_'
			}

			return r
		}, strings.TrimSpace(t))
	}

	return strings.Join(tokens, " ")
}

func (dEnv *DoltEnv) GetGRPCDialParams(config grpcendpoint.Config) (string, []grpc.DialOption, error) {
	endpoint := config.Endpoint
	if strings.IndexRune(endpoint, ':') == -1 {
		if config.Insecure {
			endpoint += ":80"
		} else {
			endpoint += ":443"
		}
	}

	var opts []grpc.DialOption
	if config.Insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		tc := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tc))
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	opts = append(opts, grpc.WithUserAgent(dEnv.getUserAgentString()))

	if config.Creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(config.Creds))
	} else if config.WithEnvCreds {
		rpcCreds, err := dEnv.getRPCCreds()
		if err != nil {
			return "", nil, err
		}
		if rpcCreds != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
	}

	return endpoint, opts, nil
}

func (dEnv *DoltEnv) GetRemotes() (map[string]Remote, error) {
	if dEnv.RSLoadErr != nil {
		return nil, dEnv.RSLoadErr
	}

	return dEnv.RepoState.Remotes, nil
}

// Check whether any backups or remotes share the given URL. Returns the first remote if multiple match.
// Returns NoRemote and false if none match.
func checkRemoteAddressConflict(url string, remotes, backups map[string]Remote) (Remote, bool) {
	for _, r := range remotes {
		if r.Url == url {
			return r, true
		}
	}
	for _, r := range backups {
		if r.Url == url {
			return r, true
		}
	}
	return NoRemote, false
}

func (dEnv *DoltEnv) AddRemote(name string, url string, fetchSpecs []string, params map[string]string) error {
	if _, ok := dEnv.RepoState.Remotes[name]; ok {
		return ErrRemoteAlreadyExists
	}

	if strings.IndexAny(name, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return ErrInvalidRemoteName
	}

	_, absRemoteUrl, err := GetAbsRemoteUrl(dEnv.FS, dEnv.Config, url)
	if err != nil {
		return fmt.Errorf("%w; %s", ErrInvalidRemoteURL, err.Error())
	}

	// can have multiple remotes with the same address, but no conflicting backups
	if r, found := checkRemoteAddressConflict(absRemoteUrl, nil, dEnv.RepoState.Backups); found {
		return fmt.Errorf("%w: '%s' -> %s", ErrRemoteAddressConflict, r.Name, r.Url)
	}

	r := Remote{name, absRemoteUrl, fetchSpecs, params, dEnv}
	dEnv.RepoState.AddRemote(r)
	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return err
	}
	return nil
}

func (dEnv *DoltEnv) GetBackups() (map[string]Remote, error) {
	if dEnv.RSLoadErr != nil {
		return nil, dEnv.RSLoadErr
	}

	return dEnv.RepoState.Backups, nil
}

func (dEnv *DoltEnv) AddBackup(name string, url string, fetchSpecs []string, params map[string]string) error {
	if _, ok := dEnv.RepoState.Backups[name]; ok {
		return ErrBackupAlreadyExists
	}

	if strings.IndexAny(name, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return ErrInvalidBackupName
	}

	_, absRemoteUrl, err := GetAbsRemoteUrl(dEnv.FS, dEnv.Config, url)
	if err != nil {
		return fmt.Errorf("%w; %s", ErrInvalidBackupURL, err.Error())
	}

	// no conflicting remote or backup addresses
	if r, found := checkRemoteAddressConflict(absRemoteUrl, dEnv.RepoState.Remotes, dEnv.RepoState.Backups); found {
		return fmt.Errorf("%w: '%s' -> %s", ErrRemoteAddressConflict, r.Name, r.Url)
	}

	r := Remote{name, absRemoteUrl, fetchSpecs, params, dEnv}
	dEnv.RepoState.AddBackup(r)
	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return err
	}
	return nil
}

func (dEnv *DoltEnv) RemoveRemote(ctx context.Context, name string) error {
	remote, ok := dEnv.RepoState.Remotes[name]
	if !ok {
		return ErrRemoteNotFound
	}

	ddb := dEnv.DoltDB
	refs, err := ddb.GetRemoteRefs(ctx)
	if err != nil {
		return ErrFailedToReadFromDb
	}

	for _, r := range refs {
		rr := r.(ref.RemoteRef)

		if rr.GetRemote() == remote.Name {
			err = ddb.DeleteBranch(ctx, rr)

			if err != nil {
				return fmt.Errorf("%w; failed to delete remote tracking ref '%s'; %s", ErrFailedToDeleteRemote, rr.String(), err.Error())
			}
		}
	}

	dEnv.RepoState.RemoveRemote(remote)
	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return ErrFailedToWriteRepoState
	}

	return nil
}

func (dEnv *DoltEnv) RemoveBackup(ctx context.Context, name string) error {
	backup, ok := dEnv.RepoState.Backups[name]
	if !ok {
		return ErrBackupNotFound
	}

	dEnv.RepoState.RemoveBackup(backup)

	err := dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return ErrFailedToWriteRepoState
	}

	return nil
}

func (dEnv *DoltEnv) GetBranches() (map[string]BranchConfig, error) {
	if dEnv.RSLoadErr != nil {
		return nil, dEnv.RSLoadErr
	}

	return dEnv.RepoState.Branches, nil
}

func (dEnv *DoltEnv) UpdateBranch(name string, new BranchConfig) error {
	if dEnv.RSLoadErr != nil {
		return dEnv.RSLoadErr
	}

	dEnv.RepoState.Branches[name] = new
	return nil
}

var ErrNotACred = errors.New("not a valid credential key id or public key")

func (dEnv *DoltEnv) FindCreds(credsDir, pubKeyOrId string) (string, error) {
	if !creds.B32CredsByteSet.ContainsAll([]byte(pubKeyOrId)) {
		return "", creds.ErrBadB32CredsEncoding
	}

	if len(pubKeyOrId) == creds.B32EncodedPubKeyLen {
		pubKeyOrId, _ = creds.PubKeyStrToKIDStr(pubKeyOrId)
	}

	if len(pubKeyOrId) != creds.B32EncodedKeyIdLen {
		return "", ErrNotACred
	}

	path := mustAbs(dEnv, credsDir, pubKeyOrId+creds.JWKFileExtension)
	exists, isDir := dEnv.FS.Exists(path)

	if isDir {
		return path, filesys.ErrIsDir
	} else if !exists {
		return "", creds.ErrCredsNotFound
	} else {
		return path, nil
	}
}

func (dEnv *DoltEnv) FindRef(ctx context.Context, refStr string) (ref.DoltRef, error) {
	localRef := ref.NewBranchRef(refStr)
	if hasRef, err := dEnv.DoltDB.HasRef(ctx, localRef); err != nil {
		return nil, err
	} else if hasRef {
		return localRef, nil
	} else {
		if strings.HasPrefix(refStr, "remotes/") {
			refStr = refStr[len("remotes/"):]
		}

		slashIdx := strings.IndexRune(refStr, '/')
		if slashIdx > 0 {
			remoteName := refStr[:slashIdx]
			if _, ok := dEnv.RepoState.Remotes[remoteName]; ok {
				remoteRef, err := ref.NewRemoteRefFromPathStr(refStr)

				if err != nil {
					return nil, err
				}

				if hasRef, err = dEnv.DoltDB.HasRef(ctx, remoteRef); err != nil {
					return nil, err
				} else if hasRef {
					return remoteRef, nil
				}
			}
		}
	}

	return nil, doltdb.ErrBranchNotFound
}

// GetRefSpecs takes an optional remoteName and returns all refspecs associated with that remote.  If "" is passed as
// the remoteName then the default remote is used.
func GetRefSpecs(rsr RepoStateReader, remoteName string) ([]ref.RemoteRefSpec, error) {
	var remote Remote
	var err error

	remotes, err := rsr.GetRemotes()
	if err != nil {
		return nil, err
	}
	if remoteName == "" {
		remote, err = GetDefaultRemote(rsr)
	} else if r, ok := remotes[remoteName]; ok {
		remote = r
	} else {
		err = ErrUnknownRemote
	}

	if err != nil {
		return nil, err
	}

	var refSpecs []ref.RemoteRefSpec
	for _, fs := range remote.FetchSpecs {
		rs, err := ref.ParseRefSpecForRemote(remote.Name, fs)

		if err != nil {
			return nil, errhand.BuildDError("error: for '%s', '%s' is not a valid refspec.", remote.Name, fs).Build()
		}

		if rrs, ok := rs.(ref.RemoteRefSpec); !ok {
			return nil, fmt.Errorf("%w; '%s' is not a valid refspec referring to a remote tracking branch", ref.ErrInvalidRefSpec, remote.Name)
		} else if rrs.GetRemote() != remote.Name {
			return nil, ErrInvalidRefSpecRemote
		} else {
			refSpecs = append(refSpecs, rrs)
		}
	}

	return refSpecs, nil
}

var ErrInvalidRefSpecRemote = errors.New("refspec refers to different remote")
var ErrNoRemote = errors.New("no remote")
var ErrUnknownRemote = errors.New("unknown remote")
var ErrCantDetermineDefault = errors.New("unable to determine the default remote")

// GetDefaultRemote gets the default remote for the environment.  Not fully implemented yet.  Needs to support multiple
// repos and a configurable default.
func GetDefaultRemote(rsr RepoStateReader) (Remote, error) {
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return NoRemote, err
	}

	if len(remotes) == 0 {
		return NoRemote, ErrNoRemote
	} else if len(remotes) == 1 {
		for _, v := range remotes {
			return v, nil
		}
	}

	if remote, ok := remotes["origin"]; ok {
		return remote, nil
	}

	return NoRemote, ErrCantDetermineDefault
}

// GetUserHomeDir returns the user's home dir
// based on current filesys
func (dEnv *DoltEnv) GetUserHomeDir() (string, error) {
	return getHomeDir(dEnv.hdp)
}

func (dEnv *DoltEnv) TempTableFilesDir() string {
	return mustAbs(dEnv, dEnv.GetDoltDir(), tempTablesDir)
}

// GetGCKeepers returns the hashes of all the objects in the environment provided that should be perserved during GC.
// TODO: this should be unnecessary since we now store the working set in a noms dataset, remove it
func GetGCKeepers(ctx context.Context, env *DoltEnv) ([]hash.Hash, error) {
	workingRoot, err := env.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return nil, err
	}

	stagedRoot, err := env.StagedRoot(ctx)
	if err != nil {
		return nil, err
	}

	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return nil, err
	}

	keepers := []hash.Hash{
		workingHash,
		stagedHash,
	}

	mergeActive, err := env.IsMergeActive(ctx)
	if err != nil {
		return nil, err
	}

	if mergeActive {
		ws, err := env.WorkingSet(ctx)
		if err != nil {
			return nil, err
		}

		cm := ws.MergeState().Commit()
		ch, err := cm.HashOf()
		if err != nil {
			return nil, err
		}

		pmw := ws.MergeState().PreMergeWorkingRoot()
		pmwh, err := pmw.HashOf()
		if err != nil {
			return nil, err
		}

		keepers = append(keepers, ch, pmwh)
	}

	return keepers, nil
}

func (dEnv *DoltEnv) DbEaFactory() editor.DbEaFactory {
	return editor.NewDbEaFactory(dEnv.TempTableFilesDir(), dEnv.DoltDB.ValueReadWriter())
}

func (dEnv *DoltEnv) BulkDbEaFactory() editor.DbEaFactory {
	return editor.NewBulkImportTEAFactory(dEnv.DoltDB.Format(), dEnv.DoltDB.ValueReadWriter(), dEnv.TempTableFilesDir())
}
