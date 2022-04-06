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
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/strhelp"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrRepositoryExists = errors.New("data repository already exists")
var ErrFailedToInitRepo = errors.New("")
var ErrFailedToCreateDirectory = errors.New("unable to create directories")
var ErrFailedToAccessDir = errors.New("unable to access directories")
var ErrFailedToCreateRepoStateWithRemote = errors.New("unable to create repo state with remote")
var ErrNoDataAtRemote = errors.New("remote at that url contains no Dolt data")
var ErrFailedToListBranches = errors.New("failed to list branches")
var ErrFailedToGetBranch = errors.New("could not get branch")
var ErrFailedToGetRootValue = errors.New("could not find root value")
var ErrFailedToResolveBranchRef = errors.New("could not resole branch ref")
var ErrFailedToCreateRemoteRef = errors.New("could not create remote ref")
var ErrFailedToDeleteBranch = errors.New("could not delete local branch after clone")
var ErrFailedToUpdateDocs = errors.New("failed to update docs on the filesystem")
var ErrUserNotFound = errors.New("could not determine user name. run dolt config --global --add user.name")
var ErrEmailNotFound = errors.New("could not determine email. run dolt config --global --add user.email")
var ErrCloneFailed = errors.New("clone failed")

func EnvForClone(ctx context.Context, nbf *types.NomsBinFormat, r env.Remote, dir string, fs filesys.Filesys, version string, homeProvider env.HomeDirProvider) (*env.DoltEnv, error) {
	exists, _ := fs.Exists(filepath.Join(dir, dbfactory.DoltDir))

	if exists {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryExists, dir)
	}

	err := fs.MkDirs(dir)

	if err != nil {
		return nil, fmt.Errorf("%w: %s; %s", ErrFailedToCreateDirectory, dir, err.Error())
	}

	err = os.Chdir(dir)

	if err != nil {
		return nil, fmt.Errorf("%w: %s; %s", ErrFailedToAccessDir, dir, err.Error())
	}

	dEnv := env.Load(ctx, homeProvider, fs, doltdb.LocalDirDoltDB, version)
	err = dEnv.InitRepoWithNoData(ctx, nbf)

	if err != nil {
		return nil, fmt.Errorf("%w; %s", ErrFailedToInitRepo, err.Error())
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

func cloneProg(eventCh <-chan pull.TableFileEvent) {
	var (
		chunks            int64
		chunksDownloading int64
		chunksDownloaded  int64
		currStats         = make(map[string]iohelp.ReadStats)
		tableFiles        = make(map[string]*nbs.TableFile)
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
				chunks += int64(tf.NumChunks())
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
			strhelp.CommaIfy(chunksDownloaded), strhelp.CommaIfy(chunks), strhelp.CommaIfy(chunksDownloading))
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

func CloneRemote(ctx context.Context, srcDB *doltdb.DoltDB, remoteName, branch string, dEnv *env.DoltEnv) error {
	eventCh := make(chan pull.TableFileEvent, 128)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneProg(eventCh)
	}()

	err := Clone(ctx, srcDB, dEnv.DoltDB, eventCh)
	close(eventCh)

	wg.Wait()

	if err != nil {
		if err == pull.ErrNoData {
			err = ErrNoDataAtRemote
		}
		return fmt.Errorf("%w; %s", ErrCloneFailed, err.Error())
	}

	branches, err := dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return fmt.Errorf("%w; %s", ErrFailedToListBranches, err.Error())
	}

	if branch == "" {
		branch = env.GetDefaultBranch(dEnv, branches)
	}

	// If we couldn't find a branch but the repo cloned successfully, it's empty. Initialize it instead of pulling from
	// the remote.
	performPull := true
	if branch == "" {
		err = InitEmptyClonedRepo(ctx, dEnv)
		if err != nil {
			return nil
		}

		branch = env.GetDefaultInitBranch(dEnv.Config)
		performPull = false
	}

	cs, _ := doltdb.NewCommitSpec(branch)
	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)

	if err != nil {
		return fmt.Errorf("%w: %s; %s", ErrFailedToGetBranch, branch, err.Error())

	}

	rootVal, err := cm.GetRootValue(ctx)
	if err != nil {
		return fmt.Errorf("%w: %s; %s", ErrFailedToGetRootValue, branch, err.Error())
	}

	// After actions.Clone, we have repository with a local branch for
	// every branch in the remote. What we want is a remote branch ref for
	// every branch in the remote. We iterate through local branches and
	// create remote refs corresponding to each of them. We delete all of
	// the local branches except for the one corresponding to |branch|.
	for _, brnch := range branches {
		cs, _ := doltdb.NewCommitSpec(brnch.GetPath())
		cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
		if err != nil {
			return fmt.Errorf("%w: %s; %s", ErrFailedToResolveBranchRef, brnch.String(), err.Error())

		}

		remoteRef := ref.NewRemoteRef(remoteName, brnch.GetPath())
		err = dEnv.DoltDB.SetHeadToCommit(ctx, remoteRef, cm)
		if err != nil {
			return fmt.Errorf("%w: %s; %s", ErrFailedToCreateRemoteRef, remoteRef.String(), err.Error())

		}

		if brnch.GetPath() != branch {
			err := dEnv.DoltDB.DeleteBranch(ctx, brnch)
			if err != nil {
				return fmt.Errorf("%w: %s; %s", ErrFailedToDeleteBranch, brnch.String(), err.Error())
			}
		}
	}

	if performPull {
		err = SaveDocsFromRoot(ctx, rootVal, dEnv)
		if err != nil {
			return ErrFailedToUpdateDocs
		}
	}

	// TODO: make this interface take a DoltRef and marshal it automatically
	err = dEnv.RepoStateWriter().SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: ref.NewBranchRef(branch)})
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branch))
	if err != nil {
		return err
	}

	ws := doltdb.EmptyWorkingSet(wsRef)
	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(rootVal).WithStagedRoot(rootVal))
	if err != nil {
		return err
	}

	return nil
}

// Inits an empty, newly cloned repo. This would be unnecessary if we properly initialized the storage for a repository
// when we created it on dolthub. If we do that, this code can be removed.
func InitEmptyClonedRepo(ctx context.Context, dEnv *env.DoltEnv) error {
	name := dEnv.Config.GetStringOrDefault(env.UserNameKey, "")
	email := dEnv.Config.GetStringOrDefault(env.UserEmailKey, "")
	initBranch := env.GetDefaultInitBranch(dEnv.Config)

	if name == "" {
		return ErrUserNotFound
	} else if email == "" {
		return ErrEmailNotFound
	}

	err := dEnv.InitDBWithTime(ctx, types.Format_Default, name, email, initBranch, datas.CommitNowFunc())
	if err != nil {
		return ErrFailedToInitRepo
	}

	return nil
}
