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
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrCantFF = errors.New("can't fast forward merge")
var ErrInvalidPullArgs = errors.New("dolt pull takes at most two args")
var ErrCannotPushRef = errors.New("cannot push ref")
var ErrFailedToDeleteRemote = errors.New("failed to delete remote")
var ErrFailedToGetRemoteDb = errors.New("failed to get remote db")
var ErrUnknownPushErr = errors.New("unknown push error")

type ProgStarter func(ctx context.Context) (*sync.WaitGroup, chan pull.Stats)
type ProgStopper func(cancel context.CancelFunc, wg *sync.WaitGroup, statsCh chan pull.Stats)

// Push will update a destination branch, in a given destination database if it can be done as a fast forward merge.
// This is accomplished first by verifying that the remote tracking reference for the source database can be updated to
// the given commit via a fast forward merge.  If this is the case, an attempt will be made to update the branch in the
// destination db to the given commit via fast forward move.  If that succeeds the tracking branch is updated in the
// source db.
func Push(ctx context.Context, tempTableDir string, mode ref.UpdateMode, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, statsCh chan pull.Stats) error {
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

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{h}, statsCh)

	if err != nil {
		return err
	}

	switch mode {
	case ref.ForceUpdate:
		err = destDB.SetHeadAndWorkingSetToCommit(ctx, destRef, commit)
		if err != nil {
			return err
		}
		err = srcDB.SetHeadToCommit(ctx, remoteRef, commit)
	case ref.FastForwardOnly:
		err = destDB.FastForwardWithWorkspaceCheck(ctx, destRef, commit)
		if err != nil {
			return err
		}
		// We set the remote ref to the commit here, regardless of its
		// previous value. It does not need to be a FastForward update
		// of the local ref for this operation to succeed.
		err = srcDB.SetHeadToCommit(ctx, remoteRef, commit)
	}

	return err
}

// DoPush returns a message about whether the push was successful for each branch or a tag.
// This includes if there is a new remote branch created, upstream is set or push was rejected for a branch.
func DoPush(ctx context.Context, pushMeta *env.PushOptions, progStarter ProgStarter, progStopper ProgStopper) (returnMsg string, err error) {
	var successPush, setUpstreamPush, failedPush []string
	for _, targets := range pushMeta.Targets {
		err = push(ctx, pushMeta.Rsr, pushMeta.TmpDir, pushMeta.SrcDb, pushMeta.DestDb, pushMeta.Remote, targets, progStarter, progStopper)
		if err == nil {
			if targets.HasUpstream {
				// TODO: should add commit hash info for branches with upstream set
				//  (e.g. 74476cf38..080b073e7  branch1 -> branch1)
			} else {
				successPush = append(successPush, fmt.Sprintf(" * [new branch]          %s -> %s", targets.SrcRef.GetPath(), targets.DestRef.GetPath()))
			}
		} else if errors.Is(err, doltdb.ErrIsAhead) || errors.Is(err, ErrCantFF) || errors.Is(err, datas.ErrMergeNeeded) {
			failedPush = append(failedPush, fmt.Sprintf(" ! [rejected]            %s -> %s (non-fast-forward)", targets.SrcRef.GetPath(), targets.DestRef.GetPath()))
			continue
		} else if !errors.Is(err, doltdb.ErrUpToDate) {
			// this will allow getting successful push messages along with the error of current push
			break
		}
		if targets.SetUpstream {
			err = pushMeta.Rsw.UpdateBranch(targets.SrcRef.GetPath(), env.BranchConfig{
				Merge: ref.MarshalableRef{
					Ref: targets.DestRef,
				},
				Remote: pushMeta.Remote.Name,
			})
			if err != nil {
				return "", err
			}
			setUpstreamPush = append(setUpstreamPush, fmt.Sprintf("branch '%s' set up to track '%s'.", targets.SrcRef.GetPath(), targets.RemoteRef.GetPath()))
		}
	}

	returnMsg, err = buildReturnMsg(successPush, setUpstreamPush, failedPush, pushMeta.Remote.Url, err)
	return
}

// push performs push on a branch or a tag.
func push(ctx context.Context, rsr env.RepoStateReader, tmpDir string, src, dest *doltdb.DoltDB, remote *env.Remote, opts *env.PushTarget, progStarter ProgStarter, progStopper ProgStopper) error {
	switch opts.SrcRef.GetType() {
	case ref.BranchRefType:
		if opts.SrcRef == ref.EmptyBranchRef {
			return deleteRemoteBranch(ctx, opts.DestRef, opts.RemoteRef, src, dest, *remote)
		} else {
			return PushToRemoteBranch(ctx, rsr, tmpDir, opts.Mode, opts.SrcRef, opts.DestRef, opts.RemoteRef, src, dest, *remote, progStarter, progStopper)
		}
	case ref.TagRefType:
		return pushTagToRemote(ctx, tmpDir, opts.SrcRef, opts.DestRef, src, dest, progStarter, progStopper)
	default:
		return fmt.Errorf("%w: %s of type %s", ErrCannotPushRef, opts.SrcRef.String(), opts.SrcRef.GetType())
	}
}

// buildReturnMsg combines the push progress information of created branches, remote tracking branches
// and rejected branches, in order. // TODO: updated branches info is missing
func buildReturnMsg(success, setUpstream, failed []string, remoteUrl string, err error) (string, error) {
	var retMsg string
	if len(success) == 0 && len(failed) == 0 {
		return "", err
	} else if len(failed) > 0 {
		err = env.ErrFailedToPush.New(remoteUrl)
	} else if errors.Is(err, doltdb.ErrUpToDate) {
		// if there are some branches with successful push
		err = nil
	}

	retMsg = fmt.Sprintf("To %s", remoteUrl)
	for _, sMsg := range success {
		retMsg = fmt.Sprintf("%s\n%s", retMsg, sMsg)
	}
	for _, fMsg := range failed {
		retMsg = fmt.Sprintf("%s\n%s", retMsg, fMsg)
	}
	for _, uMsg := range setUpstream {
		retMsg = fmt.Sprintf("%s\n%s", retMsg, uMsg)
	}
	return retMsg, err
}

// PushTag pushes a commit tag and all underlying data from a local source database to a remote destination database.
func PushTag(ctx context.Context, tempTableDir string, destRef ref.TagRef, srcDB, destDB *doltdb.DoltDB, tag *doltdb.Tag, statsCh chan pull.Stats) error {
	var err error

	addr, err := tag.GetAddr()
	if err != nil {
		return err
	}

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{addr}, statsCh)

	if err != nil {
		return err
	}

	return destDB.SetHead(ctx, destRef, addr)
}

func deleteRemoteBranch(ctx context.Context, toDelete, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) error {
	err := DeleteRemoteBranch(ctx, toDelete.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB)

	if err != nil {
		return fmt.Errorf("%w; '%s' from remote '%s'; %s", ErrFailedToDeleteRemote, toDelete.String(), remote.Name, err)
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
	headRef, err := rsr.CWBHeadRef()
	if err != nil {
		return err
	}
	cm, err := localDB.Resolve(ctx, cs, headRef)
	if err != nil {
		return fmt.Errorf("%w; refspec not found: '%s'; %s", ref.ErrInvalidRefSpec, srcRef.GetPath(), err.Error())
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, statsCh := progStarter(newCtx)
	err = Push(ctx, tempTableDir, mode, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, statsCh)
	progStopper(cancelFunc, wg, statsCh)

	switch err {
	case nil:
		cli.Println()
		return nil
	case doltdb.ErrUpToDate, doltdb.ErrIsAhead, ErrCantFF, datas.ErrMergeNeeded, datas.ErrDirtyWorkspace:
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
	wg, statsCh := progStarter(newCtx)
	err = PushTag(ctx, tempTableDir, destRef.(ref.TagRef), localDB, remoteDB, tg, statsCh)
	progStopper(cancelFunc, wg, statsCh)

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

	wsRef, err := ref.WorkingSetRefForHead(targetRef)
	if err != nil {
		return err
	}
	if hasRef {
		err = remoteDB.DeleteBranchWithWorkspaceCheck(ctx, targetRef, nil, wsRef.String())
	}

	if err != nil {
		return err
	}

	err = localDB.DeleteBranch(ctx, remoteRef, nil)

	if err != nil {
		return err
	}

	return nil
}

// FetchCommit takes a fetches a commit and all underlying data from a remote source database to the local destination database.
func FetchCommit(ctx context.Context, tempTablesDir string, srcDB, destDB *doltdb.DoltDB, srcDBCommit *doltdb.Commit, statsCh chan pull.Stats) error {
	h, err := srcDBCommit.HashOf()
	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, tempTablesDir, srcDB, []hash.Hash{h}, statsCh)
}

// FetchTag takes a fetches a commit tag and all underlying data from a remote source database to the local destination database.
func FetchTag(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, srcDBTag *doltdb.Tag, statsCh chan pull.Stats) error {
	addr, err := srcDBTag.GetAddr()
	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{addr}, statsCh)
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
		wg, statsCh := progStarter(newCtx)
		err = FetchTag(ctx, tempTableDir, srcDB, destDB, tag, statsCh)
		progStopper(cancelFunc, wg, statsCh)
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

	// The code is structured this way (different paths for progress chan v. not) so that the linter can understand there
	// isn't a context leak happening on one path
	if progStarter != nil && progStopper != nil {
		newCtx, cancelFunc := context.WithCancel(ctx)
		wg, statsCh := progStarter(newCtx)
		defer progStopper(cancelFunc, wg, statsCh)

		err = FetchCommit(ctx, tempTablesDir, srcDB, destDB, srcDBCommit, statsCh)

		if err == pull.ErrDBUpToDate {
			err = nil
		}

		if err != nil {
			return nil, err
		}

		return srcDBCommit, nil
	}

	err = FetchCommit(ctx, tempTablesDir, srcDB, destDB, srcDBCommit, nil)

	if err == pull.ErrDBUpToDate {
		err = nil
	}

	if err != nil {
		return nil, err
	}

	return srcDBCommit, nil
}

// FetchRefSpecs is the common SQL and CLI entrypoint for fetching branches, tags, and heads from a remote.
// This function takes dbData which is a env.DbData object for handling repoState read and write, and srcDB is
// a remote *doltdb.DoltDB object that is used to fetch remote branches from.
func FetchRefSpecs(
	ctx context.Context,
	dbData env.DbData,
	srcDB *doltdb.DoltDB,
	refSpecs []ref.RemoteRefSpec,
	remote env.Remote,
	mode ref.UpdateMode,
	progStarter ProgStarter,
	progStopper ProgStopper,
) error {
	var branchRefs []doltdb.RefWithHash
	err := srcDB.VisitRefsOfType(ctx, ref.HeadRefTypes, func(r ref.DoltRef, addr hash.Hash) error {
		branchRefs = append(branchRefs, doltdb.RefWithHash{Ref: r, Hash: addr})
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %s", env.ErrFailedToReadDb, err.Error())
	}

	// We build up two structures:
	// 1) The list of chunk addresses to fetch, representing the remote branch heads.
	// 2) A mapping from branch HEAD to the remote tracking ref we're going to update.

	var toFetch []hash.Hash
	var newHeads []doltdb.RefWithHash

	for _, rs := range refSpecs {
		rsSeen := false

		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef.Ref)

			if remoteTrackRef != nil {
				rsSeen = true

				toFetch = append(toFetch, branchRef.Hash)
				newHeads = append(newHeads, doltdb.RefWithHash{Ref: remoteTrackRef, Hash: branchRef.Hash})
			}
		}
		if !rsSeen {
			return fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, rs.GetRemRefToLocal())
		}
	}

	// Now we fetch all the new HEADs we need.
	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return err
	}

	err = func() error {
		newCtx := ctx
		var statsCh chan pull.Stats

		if progStarter != nil && progStopper != nil {
			var cancelFunc func()
			newCtx, cancelFunc = context.WithCancel(ctx)
			var wg *sync.WaitGroup
			wg, statsCh = progStarter(newCtx)
			defer progStopper(cancelFunc, wg, statsCh)
		}

		err = dbData.Ddb.PullChunks(ctx, tmpDir, srcDB, toFetch, statsCh)
		if err == pull.ErrDBUpToDate {
			err = nil
		}
		return err
	}()
	if err != nil {
		return err
	}

	for _, newHead := range newHeads {
		commit, err := dbData.Ddb.ReadCommit(ctx, newHead.Hash)
		if err != nil {
			return err
		}
		remoteTrackRef := newHead.Ref

		if mode.Force {
			// TODO: can't be used safely in a SQL context
			err := dbData.Ddb.SetHeadToCommit(ctx, remoteTrackRef, commit)
			if err != nil {
				return err
			}
		} else {
			ok, err := dbData.Ddb.CanFastForward(ctx, remoteTrackRef, commit)
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
				err = dbData.Ddb.FastForward(ctx, remoteTrackRef, commit)
				if err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
					return fmt.Errorf("%w: %s", ErrCantFF, err.Error())
				}
			default:
				return fmt.Errorf("%w: %s", ErrCantFF, err.Error())
			}
		}
	}

	if mode.Prune {
		err = pruneBranches(ctx, dbData, remote, newHeads)
		if err != nil {
			return err
		}
	}

	err = FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, progStarter, progStopper)
	if err != nil {
		return err
	}

	return nil
}

func pruneBranches(ctx context.Context, dbData env.DbData, remote env.Remote, remoteRefs []doltdb.RefWithHash) error {
	remoteRefTypes := map[ref.RefType]struct{}{
		ref.RemoteRefType: {},
	}

	var localRemoteRefs []ref.RemoteRef
	err := dbData.Ddb.VisitRefsOfType(ctx, remoteRefTypes, func(r ref.DoltRef, addr hash.Hash) error {
		rref := r.(ref.RemoteRef)
		localRemoteRefs = append(localRemoteRefs, rref)
		return nil
	})
	if err != nil {
		return err
	}

	// Delete any local remote ref not present in the remoteRefs, only for this remote
	for _, localRemoteRef := range localRemoteRefs {
		if localRemoteRef.GetRemote() != remote.Name {
			continue
		}

		found := false
		for _, remoteRef := range remoteRefs {
			if remoteRef.Ref == localRemoteRef {
				found = true
				break
			}
		}

		if !found {
			// TODO: this isn't thread-safe in a SQL context
			err = dbData.Ddb.DeleteBranch(ctx, localRemoteRef, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SyncRoots is going to copy the root hash of the database from srcDb to destDb.
// We can do this |Clone| if (1) destDb is empty, (2) destDb and srcDb are both
// |TableFileStore|s, and (3) srcDb does *not* have a journal file. The most
// common scenario where this occurs is when we are restoring a backup.
//
// The journal's interaction with TableFileStore is not great currently ---
// when accessing a journal file through TableFileStore, the Reader() should in
// reality return something which is going to result in reading an actual table
// file. For now, we avoid the |Clone| path when the journal file is present.
func canSyncRootsWithClone(ctx context.Context, srcDb, destDb *doltdb.DoltDB, destDbRoot hash.Hash) (bool, error) {
	if !destDbRoot.IsEmpty() {
		return false, nil
	}
	if !srcDb.IsTableFileStore() {
		return false, nil
	}
	if !destDb.IsTableFileStore() {
		return false, nil
	}
	srcHasJournal, err := srcDb.TableFileStoreHasJournal(ctx)
	if err != nil {
		return false, err
	}
	if srcHasJournal {
		return false, nil
	}
	return true, nil
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
	wg, statsCh := progStarter(newCtx)
	defer func() {
		progStopper(cancelFunc, wg, statsCh)
		if err == nil {
			cli.Println()
		}
	}()

	canClone, err := canSyncRootsWithClone(ctx, srcDb, destDb, destRoot)
	if err != nil {
		return err
	}

	if canClone {
		tfCh := make(chan pull.TableFileEvent)
		go func() {
			start := time.Now()
			stats := make(map[string]iohelp.ReadStats)
			for {
				select {
				case tfe, ok := <-tfCh:
					if !ok {
						return
					}
					if tfe.EventType == pull.DownloadStats {
						stats[tfe.TableFiles[0].FileID()] = tfe.Stats[0]

						totalSentBytes := uint64(0)
						totalBytes := uint64(0)

						for _, v := range stats {
							if v.Percent > 0.001 {
								totalSentBytes += v.Read
								totalBytes += uint64(float64(v.Read) / v.Percent)
							}
						}

						// We fake some of these values.
						toEmit := pull.Stats{
							FinishedSendBytes: totalSentBytes,
							BufferedSendBytes: totalSentBytes,
							SendBytesPerSec:   float64(totalSentBytes) / (time.Since(start).Seconds()),

							// estimate the number of chunks based on an average chunk size of 4096.
							TotalSourceChunks:   totalBytes / 4096,
							FetchedSourceChunks: totalSentBytes / 4096,

							FetchedSourceBytes:       totalSentBytes,
							FetchedSourceBytesPerSec: float64(totalSentBytes) / (time.Since(start).Seconds()),
						}

						// TODO: This looks wrong without a ctx.Done() select, but Puller does not conditionally send here...
						select {
						case statsCh <- toEmit:
						}
					}
				}
			}
		}()

		err := srcDb.Clone(ctx, destDb, tfCh)
		close(tfCh)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pull.ErrCloneUnsupported) {
			return err
		}

		// If clone is unsupported, we can fall back to pull.
	}

	err = destDb.PullChunks(ctx, tempTableDir, srcDb, []hash.Hash{srcRoot}, statsCh)
	if err != nil {
		return err
	}

	var numRetries int
	var success bool
	for err == nil && !success && numRetries < 10 {
		success, err = destDb.CommitRoot(ctx, srcRoot, destRoot)
		if err == nil && !success {
			destRoot, err = destDb.NomsRoot(ctx)
			numRetries += 1
		}
	}
	if err != nil {
		return err
	}

	if !success {
		return errors.New("could not set destination root to the same value as this database's root. the destination database received too many writes while we were pushing and we exhausted our retries.")
	}

	return nil
}

func HandleInitRemoteStorageClientErr(name, url string, err error) error {
	var detail = fmt.Sprintf("the remote: %s '%s' could not be accessed", name, url)
	return fmt.Errorf("%w; %s; %s", ErrFailedToGetRemoteDb, detail, err.Error())
}

// ParseRemoteBranchName takes remote branch ref name, parses it and returns remote branch name.
// For example, it parses the input string 'origin/john/mybranch' and returns remote name 'origin' and branch name 'john/mybranch'.
func ParseRemoteBranchName(startPt string) (string, string) {
	startPt = strings.TrimPrefix(startPt, "remotes/")
	names := strings.SplitN(startPt, "/", 2)
	if len(names) < 2 {
		return "", ""
	}
	return names[0], names[1]
}

// GetRemoteBranchRef returns a remote ref with matching name for a branch for each remote.
func GetRemoteBranchRef(ctx context.Context, ddb *doltdb.DoltDB, name string) ([]ref.RemoteRef, error) {
	remoteRefFilter := map[ref.RefType]struct{}{ref.RemoteRefType: {}}
	refs, err := ddb.GetRefsOfType(ctx, remoteRefFilter)
	if err != nil {
		return nil, err
	}

	var remoteRef []ref.RemoteRef
	for _, rf := range refs {
		if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == name {
			remoteRef = append(remoteRef, remRef)
		}
	}

	return remoteRef, nil
}
