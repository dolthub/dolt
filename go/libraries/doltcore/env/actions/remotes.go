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
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
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

// PushRefResultType identifies how a pushed reference was modified on
// the remote database.
type PushRefResultType int

const (
	// PushResultTypeNewBranch indicates a new remote branch was created.
	PushResultTypeNewBranch PushRefResultType = iota
	// PushResultTypeUpdated indicates an existing remote branch was
	// fast-forwarded.
	PushResultTypeUpdated
	// PushResultTypeForced indicates an existing remote branch was
	// force-updated.
	PushResultTypeForced
	// PushResultTypeDeleted indicates a remote branch was deleted.
	PushResultTypeDeleted
	// PushResultTypeNewTag indicates a new remote tag was created.
	PushResultTypeNewTag
)

// PushRefResult records the outcome and commit hash transition of
// pushing a single reference to a remote database.
type PushRefResult struct {
	Type    PushRefResultType
	OldHash hash.Hash
	NewHash hash.Hash
}

// Push updates |destRef| on |destDB| with commits from |srcDB| and
// updates |remoteRef| in |srcDB| to track the newly pushed commit.
//
// |destRef| specifies the destination branch on |destDB|. Push
// inspects |destDB| before transferring chunks to distinguish branch
// creation from updates and returns a *PushRefResult recording the
// commit hashes. Returns ErrCantFF if a fast-forward update cannot be
// performed, [doltdb.ErrUpToDate] if the remote branch already points
// to the given commit, or an error if chunks cannot be transferred.
func Push(ctx context.Context, tempTableDir string, mode ref.UpdateMode, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, statsCh chan pull.Stats) (*PushRefResult, error) {
	var oldHash hash.Hash
	isNewBranch := false
	canFF := true
	prevCommit, err := destDB.ResolveCommitRef(ctx, destRef)
	if errors.Is(err, doltdb.ErrBranchNotFound) {
		isNewBranch = true
	} else if err != nil {
		return nil, err
	} else {
		if oldHash, err = prevCommit.HashOf(); err != nil {
			return nil, err
		}
		if canFF, err = prevCommit.CanFastForwardTo(ctx, commit); err != nil {
			return nil, err
		}
		if mode == ref.FastForwardOnly && !canFF {
			return nil, ErrCantFF
		}
	}

	h, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{h}, statsCh, nil)
	if errors.Is(err, nbs.ErrGhostChunkRequested) {
		err = ErrShallowPushImpossible
	}
	if err != nil {
		return nil, err
	}

	switch mode {
	case ref.ForceUpdate:
		err = destDB.SetHeadAndWorkingSetToCommit(ctx, destRef, commit)
		if err != nil {
			return nil, err
		}
		err = srcDB.SetHeadToCommit(ctx, remoteRef, commit)
	case ref.FastForwardOnly:
		// Working sets with only ignored tables are allowed through
		// fast-forward updates without failing the workspace check.
		onlyIgnored := false
		roots, err := destDB.ResolveBranchRoots(ctx, destRef)
		if err == nil {
			onlyIgnored, _ = diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
		}
		err = destDB.FastForwardWithWorkspaceCheck(ctx, destRef, commit, onlyIgnored)
		if err != nil {
			return nil, err
		}
		// We set the remote ref to the commit here, regardless of its
		// previous value. It does not need to be a FastForward update
		// of the local ref for this operation to succeed.
		err = srcDB.SetHeadToCommit(ctx, remoteRef, commit)
	}
	if err != nil {
		return nil, err
	}

	resultType := PushResultTypeUpdated
	if isNewBranch {
		resultType = PushResultTypeNewBranch
	} else if !canFF {
		resultType = PushResultTypeForced
	}

	return &PushRefResult{
		Type:    resultType,
		OldHash: oldHash,
		NewHash: h,
	}, nil
}

// DoPush executes a push for each target in |pushMeta| and returns a
// summary string describing created, updated, or rejected references.
//
// Targets are evaluated sequentially. If any target fails with an
// unrecoverable error, DoPush returns the accumulated output string
// along with the error.
func DoPush[C doltdb.Context](ctx C, pushMeta *env.PushOptions[C], statsCh chan pull.Stats) (returnMsg string, err error) {
	var successPush, setUpstreamPush, failedPush []string
	for _, targets := range pushMeta.Targets {
		src := targets.SrcRef.GetPath()
		dest := targets.DestRef.GetPath()
		res, pushErr := push(ctx, pushMeta.Rsr, pushMeta.TmpDir, pushMeta.SrcDb, pushMeta.DestDb, pushMeta.Remote, targets, statsCh)
		if pushErr == nil {
			successPush = append(successPush, formatPushSuccess(res, src, dest))
		} else if errors.Is(pushErr, doltdb.ErrIsAhead) || errors.Is(pushErr, ErrCantFF) || errors.Is(pushErr, datas.ErrMergeNeeded) {
			failedPush = append(failedPush, fmt.Sprintf(" ! [rejected]            %s -> %s (non-fast-forward)", src, dest))
			continue
		} else if errors.Is(pushErr, doltdb.ErrUpToDate) {
			if err == nil {
				err = pushErr
			}
		} else {
			err = pushErr
			break
		}
		if targets.SetUpstream {
			err = pushMeta.Rsw.UpdateBranch(src, env.BranchConfig{
				Merge: ref.MarshalableRef{
					Ref: targets.DestRef,
				},
				Remote: pushMeta.Remote.Name,
			})
			if err != nil {
				return "", err
			}
			setUpstreamPush = append(setUpstreamPush, fmt.Sprintf("branch '%s' set up to track '%s'.", src, targets.RemoteRef.GetPath()))
		}
	}

	returnMsg, err = buildReturnMsg(successPush, setUpstreamPush, failedPush, pushMeta.Remote.Url, err)
	return
}

func formatPushSuccess(res *PushRefResult, src, dest string) string {
	switch res.Type {
	case PushResultTypeDeleted:
		return fmt.Sprintf(" - [deleted]             %s", dest)
	case PushResultTypeNewBranch:
		return fmt.Sprintf(" * [new branch]          %s -> %s", src, dest)
	case PushResultTypeNewTag:
		return fmt.Sprintf(" * [new tag]             %s -> %s", src, dest)
	case PushResultTypeForced:
		return fmt.Sprintf(" + %s...%s %s -> %s (forced update)", shortHash(res.OldHash), shortHash(res.NewHash), src, dest)
	default:
		return fmt.Sprintf("   %s..%s  %s -> %s", shortHash(res.OldHash), shortHash(res.NewHash), src, dest)
	}
}

// shortHash returns the 7-character abbreviated prefix of a commit
// hash.
func shortHash(h hash.Hash) string {
	s := h.String()
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

// push performs push on a branch or a tag.
func push[C doltdb.Context](ctx C, rsr env.RepoStateReader[C], tmpDir string, src, dest *doltdb.DoltDB, remote *env.Remote, opts *env.PushTarget, statsCh chan pull.Stats) (*PushRefResult, error) {
	switch opts.SrcRef.GetType() {
	case ref.BranchRefType:
		if opts.SrcRef == ref.EmptyBranchRef {
			if err := deleteRemoteBranch(ctx, opts.DestRef, opts.RemoteRef, src, dest, *remote, opts.Mode.Force); err != nil {
				return nil, err
			}
			return &PushRefResult{Type: PushResultTypeDeleted}, nil
		}
		return PushToRemoteBranch(ctx, rsr, tmpDir, opts.Mode, opts.SrcRef, opts.DestRef, opts.RemoteRef, src, dest, *remote, statsCh)
	case ref.TagRefType:
		if err := pushTagToRemote(ctx, tmpDir, opts.SrcRef, opts.DestRef, src, dest, statsCh); err != nil {
			return nil, err
		}
		return &PushRefResult{Type: PushResultTypeNewTag}, nil
	default:
		return nil, fmt.Errorf("%w: %s of type %s", ErrCannotPushRef, opts.SrcRef.String(), opts.SrcRef.GetType())
	}
}

// buildReturnMsg combines the push progress messages for updated,
// created, and rejected branches in order.
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

// PushToRemoteBranch resolves |srcRef| to a commit and pushes it to
// |destRef| on |remoteDB|, updating the local remote tracking
// reference |remoteRef|.
//
// Returns a *PushRefResult on success recording whether the remote
// branch was created or updated, or an error if the ref cannot be
// resolved or the push is rejected.
func PushToRemoteBranch[C doltdb.Context](ctx C, rsr env.RepoStateReader[C], tempTableDir string, mode ref.UpdateMode, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote, statsCh chan pull.Stats) (*PushRefResult, error) {
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
		return nil, err
	}
	optCmt, err := localDB.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, fmt.Errorf("%w; refspec not found: '%s'; %s", ref.ErrInvalidRefSpec, srcRef.GetPath(), err.Error())
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	res, err := Push(ctx, tempTableDir, mode, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, statsCh)
	switch err {
	case nil:
		return res, nil
	case doltdb.ErrUpToDate, doltdb.ErrIsAhead, ErrCantFF, datas.ErrMergeNeeded, datas.ErrDirtyWorkspace, ErrShallowPushImpossible:
		return nil, err
	default:
		return nil, fmt.Errorf("%w; %s", ErrUnknownPushErr, err.Error())
	}
}

func pushTagToRemote(ctx context.Context, tempTableDir string, srcRef, destRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, statsCh chan pull.Stats) error {
	tg, err := localDB.ResolveTag(ctx, srcRef.(ref.TagRef))

	if err != nil {
		return err
	}

	err = PushTag(ctx, tempTableDir, destRef.(ref.TagRef), localDB, remoteDB, tg, statsCh)
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

// FetchFollowTags fetches all tags from the source DB whose commits have already
// been fetched into the destination DB.
// todo: potentially too expensive to iterate over all srcDB tags
func FetchFollowTags(ctx context.Context, tempTableDir string, srcDB, destDB *doltdb.DoltDB, statsCh chan pull.Stats) error {
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

		err = FetchTag(ctx, tempTableDir, srcDB, destDB, t, statsCh)
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
	statsCh chan pull.Stats,
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

	err = FetchCommit(ctx, tempTablesDir, srcDB, destDB, srcDBCommit, statsCh)

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
	statsCh chan pull.Stats,
) error {

	if depth < 1 {
		return fmt.Errorf("invalid depth: %d", depth)
	}

	return fetchRefSpecsWithDepth(ctx, dbData, srcDB, []ref.RemoteRefSpec{refSpecs}, false, remote, ref.ForceUpdate, depth, statsCh)
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
	statsCh chan pull.Stats,
) error {
	return fetchRefSpecsWithDepth(ctx, dbData, srcDB, refSpecs, defaultRefSpec, remote, mode, -1, statsCh)
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
	statsCh chan pull.Stats,
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

	err = dbData.Ddb.PullChunks(ctx, tmpDir, srcDB, toFetch, statsCh, skipCmts)
	if err == pull.ErrDBUpToDate {
		err = nil
	}
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
		//
		// XXX: This used to progStarter again. statsCh might need a start / end signal.
		err = FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, statsCh)
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

// SyncRoots is going to copy the root hash of the database from srcDb
// to destDb.  Technically we should always be able to do this by
// copying table files from the source. If we see a journal file in
// the srcDb, we can convert it to table file on the fly. If we have a
// non-empty destDb, it does not technically matter - the full set of
// table files in the source are all we need at the end of the
// sync. However, it might be more efficient to Pull instead of copy
// table files if there is a non-empty dest.
//
// For now, we only clone into an empty dest. We use some heuristics
// on the size of the source store vs. the source journal, if there is
// one, to decide if we use clone or merkle dag walk.
//
// Either way, both dest and source need to be table file stores,
// since otherwise we need to pull chunks individually.
func canSyncRootsWithClone(ctx context.Context, srcDb, destDb *doltdb.DoltDB, destDbRoot hash.Hash, relationship SyncRootsDBRelationship) (bool, error) {
	if relationship != SyncRootsDBRelationshipUnrelated {
		if !destDbRoot.IsEmpty() {
			return false, nil
		}
	}
	if !srcDb.IsTableFileStore() {
		return false, nil
	}
	if !destDb.IsTableFileStore() {
		return false, nil
	}
	sizes, err := srcDb.StoreSizes(ctx)
	if err != nil {
		return false, err
	}
	if sizes.JournalBytes >= (sizes.TotalBytes / 5) {
		// The journal is more than 20% of the entire source.
		// For now we do a merkle walk instead of converting
		// all the chunks.
		return false, nil
	}
	if sizes.JournalBytes > 16*1024*1024*1024 {
		// The journal is larger than 16GB.  For now we do a
		// merkle walk instead of converting all the chunks.
		return false, nil
	}
	return true, nil
}

type SyncRootsDBRelationship int

const (
	SyncRootsDBRelationshipUnknown = iota
	SyncRootsDBRelationshipUnrelated
)

// SyncRoots copies the entire chunkstore from srcDb to destDb and rewrites the remote manifest. Used to
// streamline database backup and restores.
// TODO: this should read/write a backup lock file specific to the client who created the backup
// TODO     to prevent "restoring a remote", "cloning a backup", "syncing a remote" and "pushing
// TODO     a backup." SyncRoots has more destructive potential than push right now.
func SyncRoots(ctx context.Context, srcDb, destDb *doltdb.DoltDB, tempTableDir string, relationship SyncRootsDBRelationship, statsCh chan pull.Stats) error {
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

	canClone, err := canSyncRootsWithClone(ctx, srcDb, destDb, destRoot, relationship)
	if err != nil {
		return err
	}

	if canClone {
		tfCh := make(chan pull.TableFileEvent)
		var wg sync.WaitGroup
		wg.Go(func() {
			start := time.Now()
			stats := make(map[string]iohelp.ReadStats)
			for tfe := range tfCh {
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
					statsCh <- toEmit
				}
			}
		})
		var err error
		wg.Go(func() {
			defer close(tfCh)
			err = srcDb.Clone(ctx, tempTableDir, destDb, tfCh)
		})
		wg.Wait()
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
