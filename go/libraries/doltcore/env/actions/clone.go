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

package actions

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/strhelp"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrRepositoryExists = errors.New("data repository already exists")
var ErrFailedToCreateDirectory = errors.New("unable to create directories")
var ErrFailedToAccessDir = errors.New("unable to access directories")
var ErrFailedToCreateRepoStateWithRemote = errors.New("unable to create repo state with remote")
var ErrNoDataAtRemote = errors.New("remote at that url contains no Dolt data")
var ErrFailedToListBranches = errors.New("failed to list branches")
var ErrFailedToGetBranch = errors.New("could not get branch")
var ErrFailedToGetRootValue = errors.New("could not find root value")
var ErrFailedToCreateRemoteRef = errors.New("could not create remote ref")
var ErrFailedToCreateTagRef = errors.New("could not create tag ref")
var ErrFailedToCreateLocalBranch = errors.New("could not create local branch")
var ErrFailedToDeleteBranch = errors.New("could not delete local branch after clone")
var ErrUserNotFound = errors.New("could not determine user name. run dolt config --global --add user.name")
var ErrEmailNotFound = errors.New("could not determine email. run dolt config --global --add user.email")
var ErrCloneFailed = errors.New("clone failed")

// EnvForClone creates a new DoltEnv and configures it with repo state from the specified remote. The returned DoltEnv is ready for content to be cloned into it. The directory used for the new DoltEnv is determined by resolving the specified dir against the specified Filesys.
func EnvForClone(ctx context.Context, nbf *types.NomsBinFormat, r env.Remote, dir string, fs filesys.Filesys, version string, homeProvider env.HomeDirProvider) (*env.DoltEnv, error) {
	exists, _ := fs.Exists(filepath.Join(dir, dbfactory.DoltDir))

	if exists {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryExists, dir)
	}

	err := fs.MkDirs(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateDirectory, dir, err.Error())
	}

	newFs, err := fs.WithWorkingDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: %s; %s", ErrFailedToAccessDir, dir, err.Error())
	}

	dEnv := env.Load(ctx, homeProvider, newFs, doltdb.LocalDirDoltDB, version)
	if errors.Is(dEnv.DBLoadError, doltdb.ErrMissingDoltDataDir) {
		err = dEnv.InitRepoWithNoData(ctx, nbf)
		if err != nil {
			return nil, fmt.Errorf("failed to init repo: %w", err)
		}
	}

	dEnv.RSLoadErr = nil
	if !env.IsEmptyRemote(r) {
		dEnv.RepoState, err = env.CloneRepoState(dEnv.FS, r)
		if err != nil {
			return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateRepoStateWithRemote, r.Name, err.Error())
		}
	}

	return dEnv, nil
}

func clonePrint(eventCh <-chan pull.TableFileEvent) {
	var (
		chunksC           int64
		chunksDownloading int64
		chunksDownloaded  int64
		currStats         = make(map[string]iohelp.ReadStats)
		tableFiles        = make(map[string]*chunks.TableFile)
	)

	p := cli.NewEphemeralPrinter()

	p.Printf("Retrieving remote information.\n")
	p.Display()

	for tblFEvt := range eventCh {
		switch tblFEvt.EventType {
		case pull.Listed:
			for _, tf := range tblFEvt.TableFiles {
				c := tf
				tableFiles[c.FileID()] = &c
				chunksC += int64(tf.NumChunks())
			}
		case pull.DownloadStart:
			for _, tf := range tblFEvt.TableFiles {
				chunksDownloading += int64(tf.NumChunks())
			}
		case pull.DownloadStats:
			for i, s := range tblFEvt.Stats {
				tf := tblFEvt.TableFiles[i]
				currStats[tf.FileID()] = s
			}
		case pull.DownloadSuccess:
			for _, tf := range tblFEvt.TableFiles {
				chunksDownloading -= int64(tf.NumChunks())
				chunksDownloaded += int64(tf.NumChunks())
				delete(currStats, tf.FileID())
			}
		case pull.DownloadFailed:
			// Ignore for now and output errors on the main thread
			for _, tf := range tblFEvt.TableFiles {
				delete(currStats, tf.FileID())
			}
		}

		p.Printf("%s of %s chunks complete. %s chunks being downloaded currently.\n",
			strhelp.CommaIfy(chunksDownloaded), strhelp.CommaIfy(chunksC), strhelp.CommaIfy(chunksDownloading))
		for _, fileId := range sortedKeys(currStats) {
			s := currStats[fileId]
			bps := float64(s.Read) / s.Elapsed.Seconds()
			rate := humanize.Bytes(uint64(bps)) + "/s"
			p.Printf("Downloading file: %s (%s chunks) - %.2f%% downloaded, %s\n",
				fileId, strhelp.CommaIfy(int64((*tableFiles[fileId]).NumChunks())), s.Percent*100, rate)
		}
		p.Display()
	}
	p.Display()
}

func sortedKeys(m map[string]iohelp.ReadStats) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// CloneRemote - common entry point for both dolt_clone() and `dolt clone`
// The database must be initialized with a remote before calling this function.
//
// The `branch` parameter is the branch to clone. If it is empty, the default branch is used.
func CloneRemote(ctx context.Context, srcDB *doltdb.DoltDB, remoteName, branch string, singleBranch bool, depth int, dEnv *env.DoltEnv) error {
	// We support two forms of cloning: full and shallow. These two approaches have little in common, with the exception
	// of the first and last steps. Determining the branch to check out and setting the working set to the checked out commit.

	srcRefHashes, branch, err := getSrcRefs(ctx, branch, srcDB, dEnv)
	if err != nil {
		return fmt.Errorf("%w; %s", ErrCloneFailed, err.Error())
	}
	if remoteName == "" {
		remoteName = "origin"
	}

	var checkedOutCommit *doltdb.Commit

	// Step 1) Pull the remote information we care about to a local disk.
	if depth <= 0 {
		checkedOutCommit, err = fullClone(ctx, srcDB, dEnv, srcRefHashes, branch, remoteName, singleBranch)
	} else {
		checkedOutCommit, err = shallowCloneDataPull(ctx, dEnv.DbData(), srcDB, remoteName, branch, depth)
	}

	if err != nil {
		if err == pull.ErrNoData {
			err = ErrNoDataAtRemote
		}
		return fmt.Errorf("%w; %s", ErrCloneFailed, err.Error())
	}

	// TODO: make this interface take a DoltRef and marshal it automatically
	err = dEnv.RepoStateWriter().SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: ref.NewBranchRef(branch)})
	if err != nil {
		return err
	}

	rootVal, err := checkedOutCommit.GetRootValue(ctx)
	if err != nil {
		return fmt.Errorf("%w: %s; %s", ErrFailedToGetRootValue, branch, err.Error())
	}

	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branch))
	if err != nil {
		return err
	}

	// Retrieve existing working set, delete if it exists
	ws, err := dEnv.DoltDB.ResolveWorkingSet(ctx, wsRef)
	if ws != nil {
		dEnv.DoltDB.DeleteWorkingSet(ctx, wsRef)
	}
	ws = doltdb.EmptyWorkingSet(wsRef)

	// Update to use current Working and Staged root
	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(rootVal).WithStagedRoot(rootVal))
	if err != nil {
		return err
	}

	return nil
}

// getSrcRefs returns the refs from the source database and the branch to check out. The input branch is used if it is
// not empty, otherwise the default branch is determined and returned.
func getSrcRefs(ctx context.Context, branch string, srcDB *doltdb.DoltDB, dEnv *env.DoltEnv) ([]doltdb.RefWithHash, string, error) {
	srcRefHashes, err := srcDB.GetRefsWithHashes(ctx)
	if err != nil {
		return nil, "", err
	}

	if len(srcRefHashes) == 0 {
		return nil, "", ErrNoDataAtRemote
	}

	branches := make([]ref.DoltRef, 0, len(srcRefHashes))
	for _, refHash := range srcRefHashes {
		if refHash.Ref.GetType() == ref.BranchRefType {
			br := refHash.Ref.(ref.BranchRef)
			branches = append(branches, br)
		}
	}
	if branch == "" {
		branch = env.GetDefaultBranch(dEnv, branches)
	}

	return srcRefHashes, branch, nil
}

func fullClone(ctx context.Context, srcDB *doltdb.DoltDB, dEnv *env.DoltEnv, srcRefHashes []doltdb.RefWithHash, branch, remoteName string, singleBranch bool) (*doltdb.Commit, error) {
	eventCh := make(chan pull.TableFileEvent, 128)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		clonePrint(eventCh)
	}()

	err := srcDB.Clone(ctx, dEnv.DoltDB, eventCh)

	close(eventCh)
	wg.Wait()

	if err != nil {
		return nil, err
	}

	cs, _ := doltdb.NewCommitSpec(branch)
	optCmt, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	err = dEnv.DoltDB.DeleteAllRefs(ctx)
	if err != nil {
		return nil, err
	}

	// Preserve only branch and tag references from the remote. Branches are translated into remote branches, tags are preserved.
	for _, refHash := range srcRefHashes {
		if refHash.Ref.GetType() == ref.BranchRefType {
			br := refHash.Ref.(ref.BranchRef)
			if !singleBranch || br.GetPath() == branch {
				remoteRef := ref.NewRemoteRef(remoteName, br.GetPath())
				err = dEnv.DoltDB.SetHead(ctx, remoteRef, refHash.Hash)
				if err != nil {
					return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateRemoteRef, remoteRef.String(), err.Error())

				}
			}
			if br.GetPath() == branch {
				// This is the only local branch after the clone is complete.
				err = dEnv.DoltDB.SetHead(ctx, br, refHash.Hash)
				if err != nil {
					return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateLocalBranch, br.String(), err.Error())
				}
			}
		} else if refHash.Ref.GetType() == ref.TagRefType {
			tr := refHash.Ref.(ref.TagRef)
			err = dEnv.DoltDB.SetHead(ctx, tr, refHash.Hash)
			if err != nil {
				return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateTagRef, tr.String(), err.Error())
			}
		}
	}

	return cm, nil
}

// shallowCloneDataPull is a shallow clone specific helper function to pull only the data required to show the given branch
// at the depth given.
func shallowCloneDataPull(ctx context.Context, destData env.DbData, srcDB *doltdb.DoltDB, remoteName, branch string, depth int) (*doltdb.Commit, error) {
	remotes, err := destData.Rsr.GetRemotes()
	if err != nil {
		return nil, err
	}
	remote, ok := remotes.Get(remoteName)
	if !ok {
		// By the time we get to this point, the remote should be created, so this should never happen.
		return nil, fmt.Errorf("remote %s not found", remoteName)
	}

	specs, _, err := env.ParseRefSpecs([]string{branch}, destData.Rsr, remote)
	if err != nil {
		return nil, err
	}

	err = ShallowFetchRefSpec(ctx, destData, srcDB, specs[0], &remote, depth)
	if err != nil {
		return nil, err
	}

	// After the fetch approach, we just need to create the local branch. The single remote branch already exists.
	br := ref.NewBranchRef(branch)

	cmt, err := srcDB.ResolveCommitRef(ctx, br)
	if err != nil {
		return nil, err
	}

	hsh, err := cmt.HashOf()
	if err != nil {
		return nil, err
	}

	// This is the only local branch after the clone is complete.
	err = destData.Ddb.SetHead(ctx, br, hsh)
	if err != nil {
		return nil, err
	}

	return cmt, nil
}

// InitEmptyClonedRepo inits an empty, newly cloned repo. This would be unnecessary if we properly initialized the
// storage for a repository when we created it on dolthub. If we do that, this code can be removed.
func InitEmptyClonedRepo(ctx context.Context, dEnv *env.DoltEnv) error {
	name := dEnv.Config.GetStringOrDefault(config.UserNameKey, "")
	email := dEnv.Config.GetStringOrDefault(config.UserEmailKey, "")
	initBranch := env.GetDefaultInitBranch(dEnv.Config)

	if name == "" {
		return ErrUserNotFound
	} else if email == "" {
		return ErrEmailNotFound
	}

	err := dEnv.InitDBWithTime(ctx, types.Format_Default, name, email, initBranch, datas.CommitterDate())
	if err != nil {
		return fmt.Errorf("failed to init repo: %w", err)
	}

	return nil
}
