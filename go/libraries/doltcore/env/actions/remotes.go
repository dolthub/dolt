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

package actions

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

var ErrCantFF = errors.New("can't fast forward merge")
var ErrInvalidPullArgs = errors.New("dolt pull takes at most one arg")
var ErrCannotPushRef = errors.New("cannot push ref")
var ErrFailedToSaveRepoState = errors.New("failed to save repo state")
var ErrFailedToDeleteRemote = errors.New("failed to delete remote")
var ErrFailedToGetRemoteDb = errors.New("failed to get remote db")
var ErrFailedToDeleteBackup = errors.New("failed to delete backup")
var ErrFailedToGetBackupDb = errors.New("failed to get backup db")
var ErrUnknownPushErr = errors.New("unknown push error")

type ProgStarter func(ctx context.Context) (*sync.WaitGroup, chan pull.PullProgress, chan pull.Stats)
type ProgStopper func(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan pull.PullProgress, statsCh chan pull.Stats)

// Push will update a destination branch, in a given destination database if it can be done as a fast forward merge.
// This is accomplished first by verifying that the remote tracking reference for the source database can be updated to
// the given commit via a fast forward merge.  If this is the case, an attempt will be made to update the branch in the
// destination db to the given commit via fast forward move.  If that succeeds the tracking branch is updated in the
// source db.
func Push(ctx context.Context, tempTableDir string, mode ref.UpdateMode, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan pull.PullProgress, statsCh chan pull.Stats) error {
	var err error
	if mode == ref.FastForwardOnly {
		canFF, err := srcDB.CanFastForward(ctx, remoteRef, commit)

		if err != nil {
			return err
		} else if !canFF {
			return ErrCantFF
		}
	}

	h, err := commit.HashOf()
	if err != nil {
		return err
	}

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, h, progChan, statsCh)

	if err != nil {
		return err
	}

	switch mode {
	case ref.ForceUpdate:
		err = destDB.SetHeadToCommit(ctx, destRef, commit)
		if err != nil {
			return err
		}
		err = srcDB.SetHeadToCommit(ctx, remoteRef, commit)
	case ref.FastForwardOnly:
		err = destDB.FastForward(ctx, destRef, commit)
		if err != nil {
			return err
		}
		err = srcDB.FastForward(ctx, remoteRef, commit)
	}

	return err
}

func DoPush(ctx context.Context, rsr env.RepoStateReader, rsw env.RepoStateWriter, srcDB *doltdb.DoltDB, tempTableDir string, opts *env.PushOpts, progStarter ProgStarter, progStopper ProgStopper) error {
	destDB, err := opts.Remote.GetRemoteDB(ctx, srcDB.ValueReadWriter().Format())

	if err != nil {
		if err == remotestorage.ErrInvalidDoltSpecPath {
			urlObj, _ := earl.Parse(opts.Remote.Url)
			path := urlObj.Path
			if path[0] == '/' {
				path = path[1:]
			}

			var detail = fmt.Sprintf("the remote: %s %s '%s' should be in the format 'organization/repo'", opts.Remote.Name, opts.Remote.Url, path)
			return fmt.Errorf("%w; %s; %s", ErrFailedToGetRemoteDb, detail, err.Error())
		}
		return err
	}

	switch opts.SrcRef.GetType() {
	case ref.BranchRefType:
		if opts.SrcRef == ref.EmptyBranchRef {
			err = deleteRemoteBranch(ctx, opts.DestRef, opts.RemoteRef, srcDB, destDB, opts.Remote)
		} else {
			err = PushToRemoteBranch(ctx, rsr, tempTableDir, opts.Mode, opts.SrcRef, opts.DestRef, opts.RemoteRef, srcDB, destDB, opts.Remote, progStarter, progStopper)
		}
	case ref.TagRefType:
		err = pushTagToRemote(ctx, tempTableDir, opts.SrcRef, opts.DestRef, srcDB, destDB, progStarter, progStopper)
	default:
		err = fmt.Errorf("%w: %s of type %s", ErrCannotPushRef, opts.SrcRef.String(), opts.SrcRef.GetType())
	}

	if err == nil || errors.Is(err, doltdb.ErrUpToDate) || errors.Is(err, pull.ErrDBUpToDate) {
		if opts.SetUpstream {
			err := rsw.UpdateBranch(opts.SrcRef.GetPath(), env.BranchConfig{
				Merge: ref.MarshalableRef{
					Ref: opts.DestRef,
				},
				Remote: opts.Remote.Name,
			})
			if err != nil {
				return err
			}
		}
	}

	return err
}

// PushTag pushes a commit tag and all underlying data from a local source database to a remote destination database.
func PushTag(ctx context.Context, tempTableDir string, destRef ref.TagRef, srcDB, destDB *doltdb.DoltDB, tag *doltdb.Tag, progChan chan pull.PullProgress, statsCh chan pull.Stats) error {
	var err error

	addr, err := tag.GetAddr()
	if err != nil {
		return err
	}

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, addr, progChan, statsCh)

	if err != nil {
		return err
	}

	return destDB.SetHead(ctx, destRef, addr)
}

func deleteRemoteBranch(ctx context.Context, toDelete, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) error {
	err := DeleteRemoteBranch(ctx, toDelete.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB)

	if err != nil {
		return fmt.Errorf("%w; '%s' from remote '%s'", ErrFailedToDeleteRemote, toDelete.String(), remote.Name)
		//return err
	}

	return nil
}

func PushToRemoteBranch(ctx context.Context, rsr env.RepoStateReader, tempTableDir string, mode ref.UpdateMode, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote, progStarter ProgStarter, progStopper ProgStopper) error {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(remote.Url)

	// TODO: why is evt nil sometimes?
	if err == nil && evt != nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.GetPath())
	cm, err := localDB.Resolve(ctx, cs, rsr.CWBHeadRef())

	if err != nil {
		return fmt.Errorf("%w; refspec not found: '%s'; %s", ref.ErrInvalidRefSpec, srcRef.GetPath(), err.Error())
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, statsCh := progStarter(newCtx)
	err = Push(ctx, tempTableDir, mode, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, progChan, statsCh)
	progStopper(cancelFunc, wg, progChan, statsCh)

	switch err {
	case nil:
		cli.Println()
		return nil
	case doltdb.ErrUpToDate, doltdb.ErrIsAhead, ErrCantFF, datas.ErrMergeNeeded:
		return err
	default:
		return fmt.Errorf("%w; %s", ErrUnknownPushErr, err.Error())
	}
}

func pushTagToRemote(ctx context.Context, tempTableDir string, srcRef, destRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, progStarter ProgStarter, progStopper ProgStopper) error {
	tg, err := localDB.ResolveTag(ctx, srcRef.(ref.TagRef))

	if err != nil {
		return err
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, statsCh := progStarter(newCtx)
	err = PushTag(ctx, tempTableDir, destRef.(ref.TagRef), localDB, remoteDB, tg, progChan, statsCh)
	progStopper(cancelFunc, wg, progChan, statsCh)

	if err != nil {
		return err
	}

	cli.Println()
	return nil
}

// DeleteRemoteBranch validates targetRef is a branch on the remote database, and then deletes it, then deletes the
// remote tracking branch from the local database.
func DeleteRemoteBranch(ctx context.Context, targetRef ref.BranchRef, remoteRef ref.RemoteRef, localDB, remoteDB *doltdb.DoltDB) error {
	hasRef, err := remoteDB.HasRef(ctx, targetRef)

	if err != nil {
		return err
	}

	if hasRef {
		err = remoteDB.DeleteBranch(ctx, targetRef)
	}

	if err != nil {
		return err
	}

	err = localDB.DeleteBranch(ctx, remoteRef)

	if err != nil {
		return err
	}

	return nil
}

// FetchCommit takes a fetches a commit and all underlying data from a remote source database to the local destination database.
func FetchCommit(ctx context.Context, tempTablesDir string, srcDB, destDB *doltdb.DoltDB, srcDBCommit *doltdb.Commit, progChan chan pull.PullProgress, statsCh chan pull.Stats) error {
	h, err := srcDBCommit.HashOf()
	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, tempTablesDir, srcDB, h, progChan, statsCh)
}

// FetchTag takes a fetches a commit tag and all underlying data from a remote source database to the local destination database.
func FetchTag(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, srcDBTag *doltdb.Tag, progChan chan pull.PullProgress, statsCh chan pull.Stats) error {
	addr, err := srcDBTag.GetAddr()
	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, tempTableDir, srcDB, addr, progChan, statsCh)
}

// Clone pulls all data from a remote source database to a local destination database.
func Clone(ctx context.Context, srcDB, destDB *doltdb.DoltDB, eventCh chan<- pull.TableFileEvent) error {
	return srcDB.Clone(ctx, destDB, eventCh)
}

// FetchFollowTags fetches all tags from the source DB whose commits have already
// been fetched into the destination DB.
// todo: potentially too expensive to iterate over all srcDB tags
func FetchFollowTags(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, progStarter ProgStarter, progStopper ProgStopper) error {
	err := IterResolvedTags(ctx, srcDB, func(tag *doltdb.Tag) (stop bool, err error) {
		tagHash, err := tag.GetAddr()
		if err != nil {
			return true, err
		}

		has, err := destDB.Has(ctx, tagHash)
		if err != nil {
			return true, err
		}
		if has {
			// tag is already fetched
			return false, nil
		}

		cmHash, err := tag.Commit.HashOf()
		if err != nil {
			return true, err
		}

		has, err = destDB.Has(ctx, cmHash)
		if err != nil {
			return true, err
		}
		if !has {
			// neither tag nor commit has been fetched
			return false, nil
		}

		newCtx, cancelFunc := context.WithCancel(ctx)
		wg, progChan, statsCh := progStarter(newCtx)
		err = FetchTag(ctx, tempTableDir, srcDB, destDB, tag, progChan, statsCh)
		progStopper(cancelFunc, wg, progChan, statsCh)
		if err == nil {
			cli.Println()
		} else if err == pull.ErrDBUpToDate {
			err = nil
		}

		if err != nil {
			return true, err
		}

		err = destDB.SetHead(ctx, tag.GetDoltRef(), tagHash)

		return false, err
	})

	if err != nil {
		return err
	}

	return nil
}

// FetchRemoteBranch fetches and returns the |Commit| corresponding to the remote ref given. Returns an error if the
// remote reference doesn't exist or can't be fetched. Blocks until the fetch is complete.
func FetchRemoteBranch(
	ctx context.Context,
	tempTablesDir string,
	rem env.Remote,
	srcDB, destDB *doltdb.DoltDB,
	srcRef ref.DoltRef,
	progStarter ProgStarter,
	progStopper ProgStopper,
) (*doltdb.Commit, error) {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(rem.Url)

	if err == nil && evt != nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.String())
	srcDBCommit, err := srcDB.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, fmt.Errorf("unable to find '%s' on '%s'; %w", srcRef.GetPath(), rem.Name, err)
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, statsCh := progStarter(newCtx)
	err = FetchCommit(ctx, tempTablesDir, srcDB, destDB, srcDBCommit, progChan, statsCh)
	progStopper(cancelFunc, wg, progChan, statsCh)
	if err == pull.ErrDBUpToDate {
		err = nil
	}

	if err != nil {
		return nil, err
	}

	return srcDBCommit, nil
}

// FetchRefSpecs is the common SQL and CLI entrypoint for fetching branches, tags, and heads from a remote.
func FetchRefSpecs(ctx context.Context, dbData env.DbData, refSpecs []ref.RemoteRefSpec, remote env.Remote, mode ref.UpdateMode, progStarter ProgStarter, progStopper ProgStopper) error {
	srcDB, err := remote.GetRemoteDBWithoutCaching(ctx, dbData.Ddb.ValueReadWriter().Format())
	if err != nil {
		return err
	}

	branchRefs, err := srcDB.GetHeadRefs(ctx)
	if err != nil {
		return env.ErrFailedToReadDb
	}

	for _, rs := range refSpecs {
		rsSeen := false

		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef)

			if remoteTrackRef != nil {
				rsSeen = true
				srcDBCommit, err := FetchRemoteBranch(ctx, dbData.Rsw.TempTableFilesDir(), remote, srcDB, dbData.Ddb, branchRef, progStarter, progStopper)
				if err != nil {
					return err
				}

				switch mode {
				case ref.ForceUpdate:
					// TODO: can't be used safely in a SQL context
					err := dbData.Ddb.SetHeadToCommit(ctx, remoteTrackRef, srcDBCommit)
					if err != nil {
						return err
					}
				case ref.FastForwardOnly:
					ok, err := dbData.Ddb.CanFastForward(ctx, remoteTrackRef, srcDBCommit)
					if err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
						return fmt.Errorf("%w: %s", ErrCantFF, err.Error())
					}
					if !ok {
						return ErrCantFF
					}

					switch err {
					case doltdb.ErrUpToDate:
					case doltdb.ErrIsAhead, nil:
						// TODO: can't be used safely in a SQL context
						err = dbData.Ddb.FastForward(ctx, remoteTrackRef, srcDBCommit)
						if err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
							return fmt.Errorf("%w: %s", ErrCantFF, err.Error())
						}
					default:
						return fmt.Errorf("%w: %s", ErrCantFF, err.Error())
					}
				}
			}
		}
		if !rsSeen {
			return fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, rs.GetRemRefToLocal())
		}
	}

	err = FetchFollowTags(ctx, dbData.Rsw.TempTableFilesDir(), srcDB, dbData.Ddb, progStarter, progStopper)
	if err != nil {
		return err
	}

	return nil
}

// SyncRoots copies the entire chunkstore from srcDb to destDb and rewrites the remote manifest. Used to
// streamline database backup and restores.
// TODO: this should read/write a backup lock file specific to the client who created the backup
// TODO     to prevent "restoring a remote", "cloning a backup", "syncing a remote" and "pushing
// TODO     a backup." SyncRoots has more destructive potential than push right now.
func SyncRoots(ctx context.Context, srcDb, destDb *doltdb.DoltDB, tempTableDir string, progStarter ProgStarter, progStopper ProgStopper) error {
	srcRoot, err := srcDb.NomsRoot(ctx)
	if err != nil {
		return nil
	}

	destRoot, err := destDb.NomsRoot(ctx)
	if err != nil {
		return err
	}

	if srcRoot == destRoot {
		return pull.ErrDBUpToDate
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, statsCh := progStarter(newCtx)
	defer func() {
		progStopper(cancelFunc, wg, progChan, statsCh)
		if err == nil {
			cli.Println()
		}
	}()

	err = destDb.PullChunks(ctx, tempTableDir, srcDb, srcRoot, progChan, statsCh)
	if err != nil {
		return err
	}

	destDb.CommitRoot(ctx, srcRoot, destRoot)

	return nil
}
