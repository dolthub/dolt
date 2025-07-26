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

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

var ErrCantFF = errors.New("can't fast forward merge")
var ErrInvalidPullArgs = errors.New("dolt pull takes at most two args")
var ErrCannotPushRef = errors.New("cannot push ref")
var ErrFailedToDeleteRemote = errors.New("failed to delete remote")
var ErrFailedToGetRemoteDb = errors.New("failed to get remote db")
var ErrUnknownPushErr = errors.New("unknown push error")
var ErrShallowPushImpossible = errors.New("shallow repository missing chunks to complete push")

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
		canFF, err := destDB.CanFastForward(ctx, destRef, commit)

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

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{h}, statsCh, nil)

	if errors.Is(err, nbs.ErrGhostChunkRequested) {
		err = ErrShallowPushImpossible
	}

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
func DoPush[C doltdb.Context](ctx C, pushMeta *env.PushOptions[C], progStarter ProgStarter, progStopper ProgStopper) (returnMsg string, err error) {
	var successPush, setUpstreamPush, failedPush []string
	for _, targets := range pushMeta.Targets {
		err = push(ctx, pushMeta.Rsr, pushMeta.TmpDir, pushMeta.SrcDb, pushMeta.DestDb, pushMeta.Remote, targets, progStarter, progStopper)
		if err == nil {
			// TODO: we don't have sufficient information here to know what actually happened in the push. Supporting
			// git behavior of printing the commit ids updated (e.g. 74476cf38..080b073e7  branch1 -> branch1) isn't
			// currently possible. We need to plumb through results in the return from the Push(). Having just an error
			// response is not sufficient, as there are many "success" cases that are not errors.
			if targets.SrcRef == ref.EmptyBranchRef {
				successPush = append(successPush, fmt.Sprintf(" - [deleted]             %s", targets.DestRef.GetPath()))
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
func push[C doltdb.Context](ctx C, rsr env.RepoStateReader[C], tmpDir string, src, dest *doltdb.DoltDB, remote *env.Remote, opts *env.PushTarget, progStarter ProgStarter, progStopper ProgStopper) error {
	switch opts.SrcRef.GetType() {
	case ref.BranchRefType:
		if opts.SrcRef == ref.EmptyBranchRef {
			return deleteRemoteBranch(ctx, opts.DestRef, opts.RemoteRef, src, dest, *remote, opts.Mode.Force)
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

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{addr}, statsCh, nil)

	if err != nil {
		return err
	}

	return destDB.SetHead(ctx, destRef, addr)
}

func deleteRemoteBranch(ctx context.Context, toDelete, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote, force bool) error {
	err := DeleteRemoteBranch(ctx, toDelete.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, force)

	if err != nil {
		return fmt.Errorf("%w; '%s' from remote '%s'; %s", ErrFailedToDeleteRemote, toDelete.String(), remote.Name, err)
	}

	return nil
}

func PushToRemoteBranch[C doltdb.Context](ctx C, rsr env.RepoStateReader[C], tempTableDir string, mode ref.UpdateMode, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote, progStarter ProgStarter, progStopper ProgStopper) error {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(remote.Url)

	// TODO: why is evt nil sometimes?
	if err == nil && evt != nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.GetPath())
	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return err
	}
	optCmt, err := localDB.Resolve(ctx, cs, headRef)
	if err != nil {
		return fmt.Errorf("%w; refspec not found: '%s'; %s", ref.ErrInvalidRefSpec, srcRef.GetPath(), err.Error())
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, statsCh := progStarter(newCtx)
	err = Push(ctx, tempTableDir, mode, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, statsCh)
	progStopper(cancelFunc, wg, statsCh)

	switch err {
	case nil:
		return nil
	case doltdb.ErrUpToDate, doltdb.ErrIsAhead, ErrCantFF, datas.ErrMergeNeeded, datas.ErrDirtyWorkspace, ErrShallowPushImpossible:
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
func DeleteRemoteBranch(ctx context.Context, targetRef ref.BranchRef, remoteRef ref.RemoteRef, localDB, remoteDB *doltdb.DoltDB, force bool) error {
	hasRef, err := remoteDB.HasRef(ctx, targetRef)

	if err != nil {
		return err
	}

	wsRefStr := ""
	if !force {
		wsRef, err := ref.WorkingSetRefForHead(targetRef)
		if err != nil {
			return err
		}
		wsRefStr = wsRef.String()
	}

	if hasRef {
		err = remoteDB.DeleteBranchWithWorkspaceCheck(ctx, targetRef, nil, wsRefStr)
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

	return destDB.PullChunks(ctx, tempTablesDir, srcDB, []hash.Hash{h}, statsCh, nil)
}

// FetchTag takes a fetches a commit tag and all underlying data from a remote source database to the local destination database.
func FetchTag(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, srcDBTag *doltdb.Tag, statsCh chan pull.Stats) error {
	addr, err := srcDBTag.GetAddr()
	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{addr}, statsCh, nil)
}

// Clone pulls all data from a remote source database to a local destination database.
func Clone(ctx context.Context, srcDB, destDB *doltdb.DoltDB, eventCh chan<- pull.TableFileEvent) error {
	return srcDB.Clone(ctx, destDB, eventCh)
}

// FetchFollowTags fetches all tags from the source DB whose commits have already
// been fetched into the destination DB.
// todo: potentially too expensive to iterate over all srcDB tags
func FetchFollowTags(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, progStarter ProgStarter, progStopper ProgStopper) error {
	err := IterUnresolvedTags(ctx, srcDB, func(tag *doltdb.TagResolver) (stop bool, err error) {
		tagHash := tag.Addr()

		has, err := destDB.Has(ctx, tagHash)
		if err != nil {
			return true, err
		}
		if has {
			// tag is already fetched
			return false, nil
		}

		t, err := tag.Resolve(ctx)
		if err != nil {
			return true, err
		}

		cmHash, err := t.Commit.HashOf()
		if err != nil {
			return true, err
		}

		has, err = destDB.Has(ctx, cmHash)
		if err != nil {
			return true, err
		}
		if has {
			// We _might_ have it. We need to check if it's a ghost, in which case we'll skip this commit.
			optCmt, err := destDB.ReadCommit(ctx, cmHash)
			if err != nil {
				return true, err
			}
			_, ok := optCmt.ToCommit()
			if !ok {
				return false, nil
			}
		} else {
			return false, nil
		}

		newCtx, cancelFunc := context.WithCancel(ctx)
		wg, statsCh := progStarter(newCtx)
		err = FetchTag(ctx, tempTableDir, srcDB, destDB, t, statsCh)
		progStopper(cancelFunc, wg, statsCh)
		if err == nil {
			cli.Println()
		} else if err == pull.ErrDBUpToDate {
			err = nil
		}

		if err != nil {
			return true, err
		}

		err = destDB.SetHead(ctx, t.GetDoltRef(), tagHash)

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
	optCmt, err := srcDB.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to find '%s' on '%s'; %w", srcRef.GetPath(), rem.Name, err)
	}
	srcDBCommit, ok := optCmt.ToCommit()
	if !ok {
		// This really should never happen. The source db is always expected to have everything.
		return nil, doltdb.ErrGhostCommitRuntimeFailure
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

// ShallowFetchRefSpec fetches the remote refSpec from the source database to the destination database. Currently it is only
// used for shallow clones.
func ShallowFetchRefSpec[C doltdb.Context](
	ctx context.Context,
	dbData env.DbData[C],
	srcDB *doltdb.DoltDB,
	refSpecs ref.RemoteRefSpec,
	remote *env.Remote,
	depth int,
) error {

	if depth < 1 {
		return fmt.Errorf("invalid depth: %d", depth)
	}

	return fetchRefSpecsWithDepth(ctx, dbData, srcDB, []ref.RemoteRefSpec{refSpecs}, false, remote, ref.ForceUpdate, depth, nil, nil)
}

// FetchRefSpecs is the common SQL and CLI entrypoint for fetching branches, tags, and heads from a remote.
// This function takes dbData which is a env.DbData object for handling repoState read and write, and srcDB is
// a remote *doltdb.DoltDB object that is used to fetch remote branches from.
func FetchRefSpecs[C doltdb.Context](
	ctx context.Context,
	dbData env.DbData[C],
	srcDB *doltdb.DoltDB,
	refSpecs []ref.RemoteRefSpec,
	defaultRefSpec bool,
	remote *env.Remote,
	mode ref.UpdateMode,
	progStarter ProgStarter,
	progStopper ProgStopper,
) error {
	return fetchRefSpecsWithDepth(ctx, dbData, srcDB, refSpecs, defaultRefSpec, remote, mode, -1, progStarter, progStopper)
}

// fetchRefSpecsWithDepth fetches the remote refSpecs from the source database to the destination database. It fetches
// the commits and all underlying data from the source database to the destination database.
// Parameters:
// - ctx: the context
// - dbData: the env.DbData object for handling repoState read and write
// - srcDB: the remote *doltdb.DoltDB object that is used to fetch remote branches from
// - refSpecs: the list of refSpecs to fetch
// - defaultRefSpecs: a boolean that indicates whether the refSpecs are the default refSpecs. False if the user specifies anything.
// - remote: the remote object
// - mode: the ref.UpdateMode object that specifies the update mode (force or not, prune or not)
// - depth: the depth of the fetch. If depth is greater than 0, it is a shallow clone.
// - progStarter: function that starts the progress reporting
// - progStopper: function that stops the progress reporting
func fetchRefSpecsWithDepth[C doltdb.Context](
	ctx context.Context,
	dbData env.DbData[C],
	srcDB *doltdb.DoltDB,
	refSpecs []ref.RemoteRefSpec,
	defaultRefSpecs bool,
	remote *env.Remote,
	mode ref.UpdateMode,
	depth int,
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

	if len(branchRefs) == 0 {
		if defaultRefSpecs {
			// The remote has no branches. Nothing to do. Git exits silently, so we do too.
			return nil
		}
		return fmt.Errorf("no branches found in remote '%s'", remote.Name)
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

	shallowClone := depth > 0
	skipCmts := hash.NewHashSet()
	allToFetch := toFetch
	if shallowClone {
		skipCmts, err = buildInitialSkipList(ctx, srcDB, toFetch)
		if err != nil {
			return err
		}
		curToFetch := toFetch
		var newToFetch []hash.Hash
		depth--
		for skipCmts.Size() > 0 && depth > 0 {
			newToFetch, skipCmts, err = updateSkipList(ctx, srcDB, curToFetch, skipCmts)
			if err != nil {
				return err
			}

			allToFetch = append(allToFetch, newToFetch...)
			curToFetch = newToFetch
			depth--
		}
	}
	toFetch = allToFetch

	// Now we fetch all the new HEADs we need.
	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return err
	}

	if skipCmts.Size() > 0 {
		err = dbData.Ddb.PersistGhostCommits(ctx, skipCmts)
		if err != nil {
			return err
		}
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

		err = dbData.Ddb.PullChunks(ctx, tmpDir, srcDB, toFetch, statsCh, skipCmts)
		if err == pull.ErrDBUpToDate {
			err = nil
		}
		return err
	}()
	if err != nil {
		return err
	}

	for _, newHead := range newHeads {
		remoteTrackRef := newHead.Ref

		// Handle tag references differently from commit references
		if remoteTrackRef.GetType() == ref.TagRefType {
			// For tag references, use SetHead directly with the tag hash
			err := dbData.Ddb.SetHead(ctx, remoteTrackRef, newHead.Hash)
			if err != nil {
				return err
			}
		} else {
			optCmt, err := dbData.Ddb.ReadCommit(ctx, newHead.Hash)
			if err != nil {
				return err
			}
			commit, ok := optCmt.ToCommit()
			if !ok {
				// Dest DB should have each hash in `newHeads` now. If we can't read a commit, something is wrong.
				return doltdb.ErrGhostCommitRuntimeFailure
			}

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
	}

	if mode.Prune {
		err = pruneBranches(ctx, dbData, *remote, newHeads)
		if err != nil {
			return err
		}
	}

	if !shallowClone {
		// TODO: Currently shallow clones don't pull any tags, but they could. We need to make FetchFollowTags wise
		// to the skipped commits list, and then we can remove this conditional. Also, FetchFollowTags assumes that
		// progStarter and progStopper are always non-nil, which we don't assume elsewhere. Shallow clone has no
		// progress reporting, and as a result they are nil.
		err = FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, progStarter, progStopper)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildInitialSkipList(ctx context.Context, srcDB *doltdb.DoltDB, toFetch []hash.Hash) (hash.HashSet, error) {
	if len(toFetch) > 1 {
		return hash.HashSet{}, fmt.Errorf("runtime error: multiple refspecs not supported in shallow clone")
	}

	cs, err := doltdb.NewCommitSpec(toFetch[0].String())
	if err != nil {
		return hash.HashSet{}, err
	}

	allCommits, err := srcDB.BootstrapShallowResolve(ctx, cs)

	return allCommits.AsHashSet(ctx)
}

func updateSkipList(ctx context.Context, srcDB *doltdb.DoltDB, toFetch []hash.Hash, skipCmts hash.HashSet) ([]hash.Hash, hash.HashSet, error) {
	newSkipList := skipCmts.Copy()
	newFetchList := []hash.Hash{}
	for _, h := range toFetch {
		optCmt, err := srcDB.ReadCommit(ctx, h)
		if err != nil {
			return nil, nil, err
		}

		// srcDB should always be the fully populated, so if there is a ghost commit here, someone is calling this
		// function incorrectly.
		commit, ok := optCmt.ToCommit()
		if !ok {
			return nil, nil, doltdb.ErrGhostCommitEncountered
		}

		for i := 0; i < commit.NumParents(); i++ {
			parent, err := commit.GetParent(ctx, i)
			if err != nil {
				return nil, nil, err
			}
			if newSkipList.Has(parent.Addr) {
				newSkipList.Remove(parent.Addr)
				newFetchList = append(newFetchList, parent.Addr)
			}
		}

	}

	return newFetchList, newSkipList, nil
}

func pruneBranches[C doltdb.Context](ctx context.Context, dbData env.DbData[C], remote env.Remote, remoteRefs []doltdb.RefWithHash) error {
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

	err = destDb.PullChunks(ctx, tempTableDir, srcDb, []hash.Hash{srcRoot}, statsCh, nil)
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
