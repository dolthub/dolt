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
	"sort"
	"strconv"
	"sync"

	"github.com/fatih/color"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const forceFlag = "force"

var ErrCantFF = errors.New("can't fast forward merge")

type ProgStarter func() (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent)
type ProgStopper func(wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent)

// Push will update a destination branch, in a given destination database if it can be done as a fast forward merge.
// This is accomplished first by verifying that the remote tracking reference for the source database can be updated to
// the given commit via a fast forward merge.  If this is the case, an attempt will be made to update the branch in the
// destination db to the given commit via fast forward move.  If that succeeds the tracking branch is updated in the
// source db.
func Push(ctx context.Context, dEnv *env.DoltEnv, mode ref.UpdateMode, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	var err error
	if mode == ref.FastForwardOnly {
		canFF, err := srcDB.CanFastForward(ctx, remoteRef, commit)

		if err != nil {
			return err
		} else if !canFF {
			return ErrCantFF
		}
	}

	rf, err := commit.GetStRef()

	if err != nil {
		return err
	}

	err = destDB.PushChunks(ctx, dEnv.TempTableFilesDir(), srcDB, rf, progChan, pullerEventCh)

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

func DoPush(ctx context.Context, dEnv *env.DoltEnv, opts *env.PushOpts, progStarter func() (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent), progStopper func(wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent)) (verr errhand.VerboseError) {
	destDB, err := opts.Remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)

		if err == remotestorage.ErrInvalidDoltSpecPath {
			urlObj, _ := earl.Parse(opts.Remote.Url)
			bdr.AddDetails("For the remote: %s %s", opts.Remote.Name, opts.Remote.Url)

			path := urlObj.Path
			if path[0] == '/' {
				path = path[1:]
			}

			bdr.AddDetails("'%s' should be in the format 'organization/repo'", path)
		}

		return bdr.Build()
	}

	switch opts.SrcRef.GetType() {
	case ref.BranchRefType:
		if opts.SrcRef == ref.EmptyBranchRef {
			verr = deleteRemoteBranch(ctx, opts.DestRef, opts.RemoteRef, dEnv.DoltDB, destDB, opts.Remote)
		} else {
			verr = PushToRemoteBranch(ctx, dEnv, opts.Mode, opts.SrcRef, opts.DestRef, opts.RemoteRef, dEnv.DoltDB, destDB, opts.Remote, progStarter, progStopper)
		}
	case ref.TagRefType:
		verr = pushTagToRemote(ctx, dEnv, opts.SrcRef, opts.DestRef, dEnv.DoltDB, destDB, progStarter, progStopper)
	default:
		verr = errhand.BuildDError("cannot push ref %s of type %s", opts.SrcRef.String(), opts.SrcRef.GetType()).Build()
	}

	if verr != nil {
		return verr
	}

	if opts.SetUpstream {
		dEnv.RepoState.Branches[opts.SrcRef.GetPath()] = env.BranchConfig{
			Merge: ref.MarshalableRef{
				Ref: opts.DestRef,
			},
			Remote: opts.Remote.Name,
		}

		err := dEnv.RepoState.Save(dEnv.FS)

		if err != nil {
			verr = errhand.BuildDError("error: failed to save repo state").AddCause(err).Build()
		}
	}

	return verr
}

// PushTag pushes a commit tag and all underlying data from a local source database to a remote destination database.
func PushTag(ctx context.Context, dEnv *env.DoltEnv, destRef ref.TagRef, srcDB, destDB *doltdb.DoltDB, tag *doltdb.Tag, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	var err error

	rf, err := tag.GetStRef()

	if err != nil {
		return err
	}

	err = destDB.PushChunks(ctx, dEnv.TempTableFilesDir(), srcDB, rf, progChan, pullerEventCh)

	if err != nil {
		return err
	}

	return destDB.SetHead(ctx, destRef, rf)
}

func deleteRemoteBranch(ctx context.Context, toDelete, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) errhand.VerboseError {
	err := DeleteRemoteBranch(ctx, toDelete.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB)

	if err != nil {
		return errhand.BuildDError("error: failed to delete '%s' from remote '%s'", toDelete.String(), remote.Name).Build()
	}

	return nil
}

func PushToRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, mode ref.UpdateMode, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote, progStarter ProgStarter, progStopper ProgStopper) errhand.VerboseError {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(remote.Url)

	if err == nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.GetPath())
	cm, err := localDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())

	if err != nil {
		return errhand.BuildDError("error: refspec '%v' not found.", srcRef.GetPath()).Build()
	} else {
		wg, progChan, pullerEventCh := progStarter()
		err = Push(ctx, dEnv, mode, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, progChan, pullerEventCh)
		progStopper(wg, progChan, pullerEventCh)

		if err != nil {
			if err == doltdb.ErrUpToDate {
				cli.Println("Everything up-to-date")
			} else if err == doltdb.ErrIsAhead || err == ErrCantFF || err == datas.ErrMergeNeeded {
				cli.Printf("To %s\n", remote.Url)
				cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", destRef.String(), remoteRef.String())
				cli.Printf("error: failed to push some refs to '%s'\n", remote.Url)
				cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
				cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
				cli.Println("hint: 'dolt pull ...') before pushing again.")
				return errhand.BuildDError("").Build()
			} else {
				status, ok := status.FromError(err)
				if ok && status.Code() == codes.PermissionDenied {
					cli.Println("hint: have you logged into DoltHub using 'dolt login'?")
					cli.Println("hint: check that user.email in 'dolt config --list' has write perms to DoltHub repo")
				}
				if rpcErr, ok := err.(*remotestorage.RpcError); ok {
					return errhand.BuildDError("error: push failed").AddCause(err).AddDetails(rpcErr.FullDetails()).Build()
				} else {
					return errhand.BuildDError("error: push failed").AddCause(err).Build()
				}
			}
		}
	}

	return nil
}

func pushTagToRemote(ctx context.Context, dEnv *env.DoltEnv, srcRef, destRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, progStarter ProgStarter, progStopper ProgStopper) errhand.VerboseError {
	tg, err := localDB.ResolveTag(ctx, srcRef.(ref.TagRef))

	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	wg, progChan, pullerEventCh := progStarter()

	err = PushTag(ctx, dEnv, destRef.(ref.TagRef), localDB, remoteDB, tg, progChan, pullerEventCh)

	progStopper(wg, progChan, pullerEventCh)

	if err != nil {
		if err == doltdb.ErrUpToDate {
			cli.Println("Everything up-to-date")
		} else {
			return errhand.BuildDError("error: push failed").AddCause(err).Build()
		}
	}

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
func FetchCommit(ctx context.Context, dEnv *env.DoltEnv, srcDB, destDB *doltdb.DoltDB, srcDBCommit *doltdb.Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	stRef, err := srcDBCommit.GetStRef()

	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, dEnv.TempTableFilesDir(), srcDB, stRef, progChan, pullerEventCh)
}

// FetchCommit takes a fetches a commit tag and all underlying data from a remote source database to the local destination database.
func FetchTag(ctx context.Context, dEnv *env.DoltEnv, srcDB, destDB *doltdb.DoltDB, srcDBTag *doltdb.Tag, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	stRef, err := srcDBTag.GetStRef()

	if err != nil {
		return err
	}

	return destDB.PullChunks(ctx, dEnv.TempTableFilesDir(), srcDB, stRef, progChan, pullerEventCh)
}

// Clone pulls all data from a remote source database to a local destination database.
func Clone(ctx context.Context, srcDB, destDB *doltdb.DoltDB, eventCh chan<- datas.TableFileEvent) error {
	return srcDB.Clone(ctx, destDB, eventCh)
}

func PullFromRemote(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, progStarter ProgStarter, progStopper ProgStopper) errhand.VerboseError {
	if apr.NArg() > 1 {
		return errhand.BuildDError("dolt pull takes at most one arg").SetPrintUsage().Build()
	}

	branch := dEnv.RepoStateReader().CWBHeadRef()

	var remoteName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	}

	// TODO: move this logic to env
	refSpecs, verr := dEnv.GetRefSpecs(remoteName)
	if verr != nil {
		return verr
	}

	if len(refSpecs) == 0 {
		return errhand.BuildDError("error: no refspec for remote").Build()
	}

	remote := dEnv.RepoState.Remotes[refSpecs[0].GetRemote()]

	for _, refSpec := range refSpecs {
		remoteTrackRef := refSpec.DestRef(branch)

		if remoteTrackRef != nil {
			verr = pullRemoteBranch(ctx, apr, dEnv, remote, branch, remoteTrackRef, progStarter, progStopper)

			if verr != nil {
				return verr
			}
		}
	}

	srcDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	verr = FetchFollowTags(ctx, dEnv, srcDB, dEnv.DoltDB, progStarter, progStopper)

	if verr != nil {
		return verr
	}

	return nil
}

// fetchFollowTags fetches all tags from the source DB whose commits have already
// been fetched into the destination DB.
// todo: potentially too expensive to iterate over all srcDB tags
func FetchFollowTags(ctx context.Context, dEnv *env.DoltEnv, srcDB, destDB *doltdb.DoltDB, progStarter ProgStarter, progStopper ProgStopper) errhand.VerboseError {
	err := IterResolvedTags(ctx, srcDB, func(tag *doltdb.Tag) (stop bool, err error) {
		stRef, err := tag.GetStRef()
		if err != nil {
			return true, err
		}

		tagHash := stRef.TargetHash()

		tv, err := destDB.ValueReadWriter().ReadValue(ctx, tagHash)
		if err != nil {
			return true, err
		}
		if tv != nil {
			// tag is already fetched
			return false, nil
		}

		cmHash, err := tag.Commit.HashOf()
		if err != nil {
			return true, err
		}

		cv, err := destDB.ValueReadWriter().ReadValue(ctx, cmHash)
		if err != nil {
			return true, err
		}
		if cv == nil {
			// neither tag nor commit has been fetched
			return false, nil
		}

		wg, progChan, pullerEventCh := progStarter()
		err = FetchTag(ctx, dEnv, srcDB, destDB, tag, progChan, pullerEventCh)
		progStopper(wg, progChan, pullerEventCh)

		if err != nil {
			return true, err
		}

		err = destDB.SetHead(ctx, tag.GetDoltRef(), stRef)

		return false, err
	})

	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func pullRemoteBranch(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef, progStarter ProgStarter, progStopper ProgStopper) errhand.VerboseError {
	srcDB, err := r.GetRemoteDBWithoutCaching(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	srcDBCommit, verr := FetchRemoteBranch(ctx, dEnv, r, srcDB, dEnv.DoltDB, srcRef, destRef, progStarter, progStopper)

	if verr != nil {
		return verr
	}

	err = dEnv.DoltDB.FastForward(ctx, destRef, srcDBCommit)

	if err != nil {
		return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
	}

	return MergeCommitSpec(ctx, apr, dEnv, destRef.String())
}

func FetchRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, rem env.Remote, srcDB, destDB *doltdb.DoltDB, srcRef, destRef ref.DoltRef, progStarter ProgStarter, progStopper ProgStopper) (*doltdb.Commit, errhand.VerboseError) {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(rem.Url)

	if err == nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.String())
	srcDBCommit, err := srcDB.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to find '%s' on '%s'", srcRef.GetPath(), rem.Name).Build()
	} else {
		wg, progChan, pullerEventCh := progStarter()
		err = FetchCommit(ctx, dEnv, srcDB, destDB, srcDBCommit, progChan, pullerEventCh)
		progStopper(wg, progChan, pullerEventCh)

		if err != nil {
			return nil, errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		}
	}

	return srcDBCommit, nil
}

func ResolveCommitWithVErr(dEnv *env.DoltEnv, cSpecStr string) (*doltdb.Commit, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)

	if err != nil {
		return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs, dEnv.RepoStateReader().CWBHeadRef())

	if err != nil {
		if err == doltdb.ErrInvalidAncestorSpec {
			return nil, errhand.BuildDError("'%s' could not resolve ancestor spec", cSpecStr).Build()
		} else if err == doltdb.ErrBranchNotFound {
			return nil, errhand.BuildDError("unknown ref in commit spec: '%s'", cSpecStr).Build()
		} else if doltdb.IsNotFoundErr(err) {
			return nil, errhand.BuildDError("'%s' not found", cSpecStr).Build()
		} else if err == doltdb.ErrFoundHashNotACommit {
			return nil, errhand.BuildDError("'%s' is not a commit", cSpecStr).Build()
		} else {
			return nil, errhand.BuildDError("Unexpected error resolving '%s'", cSpecStr).AddCause(err).Build()
		}
	}

	return cm, nil
}

func MergeCommitSpec(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, commitSpecStr string) errhand.VerboseError {
	cm1, verr := ResolveCommitWithVErr(dEnv, "HEAD")

	if verr != nil {
		return verr
	}

	cm2, verr := ResolveCommitWithVErr(dEnv, commitSpecStr)

	if verr != nil {
		return verr
	}

	h1, err := cm1.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
	}

	h2, err := cm2.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
	}

	if h1 == h2 {
		cli.Println("Everything up-to-date")
		return nil
	}

	cli.Println("Updating", h1.String()+".."+h2.String())

	squash := apr.Contains(cli.SquashParam)
	if squash {
		cli.Println("Squash commit -- not updating HEAD")
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	tblNames, workingDiffs, err := merge.MergeWouldStompChanges(ctx, roots, cm2)

	if err != nil {
		return errhand.BuildDError("error: failed to determine mergability.").AddCause(err).Build()
	}

	if len(tblNames) != 0 {
		bldr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by merge:")
		for _, tName := range tblNames {
			bldr.AddDetails(tName)
		}
		bldr.AddDetails("Please commit your changes before you merge.")
		return bldr.Build()
	}

	if ok, err := cm1.CanFastForwardTo(ctx, cm2); ok {
		ancRoot, err := cm1.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		mergedRoot, err := cm2.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return errhand.VerboseErrorFromError(err)
		} else if cvPossible {
			return ExecuteMerge(ctx, squash, dEnv, cm1, cm2, workingDiffs)
		}
		if apr.Contains(cli.NoFFParam) {
			return ExecNoFFMerge(ctx, apr, dEnv, roots, cm2, verr, workingDiffs)
		} else {
			return ExecuteFFMerge(ctx, squash, dEnv, cm2, workingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
		return nil
	} else {
		return ExecuteMerge(ctx, squash, dEnv, cm1, cm2, workingDiffs)
	}
}

func ExecNoFFMerge(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv, roots doltdb.Roots, cm2 *doltdb.Commit, verr errhand.VerboseError, workingDiffs map[string]hash.Hash) errhand.VerboseError {
	mergedRoot, err := cm2.GetRootValue()

	if err != nil {
		return errhand.BuildDError("error: reading from database").AddCause(err).Build()
	}

	verr = mergedRootToWorking(ctx, false, dEnv, mergedRoot, workingDiffs, cm2, map[string]*merge.MergeStats{})

	if verr != nil {
		return verr
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		msg = GetCommitMessageFromEditor(ctx, dEnv)
	}

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return errhand.BuildDError("error: invalid date").AddCause(err).Build()
		}
	}

	name, email, err := GetNameAndEmail(dEnv.Config)

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	// Reload roots since the above method writes new values to the working set
	roots, err = dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(forceFlag),
		Name:       name,
		Email:      email,
	})

	if err != nil {
		return errhand.BuildDError("error: committing").AddCause(err).Build()
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, errhand.VerboseError) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
		}
	}

	return root, nil
}

// printSuccessStats returns whether there are conflicts or constraint violations.
func printSuccessStats(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	printModifications(tblToStats)
	printAdditions(tblToStats)
	printDeletions(tblToStats)
	return printConflictsAndViolations(tblToStats)
}

func printAdditions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableRemoved {
			cli.Println(tblName, "added")
		}
	}
}

func printDeletions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableRemoved {
			cli.Println(tblName, "deleted")
		}
	}
}

func printConflictsAndViolations(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	hasConflicts := false
	hasConstraintViolations := false
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && (stats.Conflicts > 0 || stats.ConstraintViolations > 0) {
			cli.Println("Auto-merging", tblName)
			if stats.Conflicts > 0 {
				cli.Println("CONFLICT (content): Merge conflict in", tblName)
				hasConflicts = true
			}
			if stats.ConstraintViolations > 0 {
				cli.Println("CONSTRAINT VIOLATION (content): Merge created constraint violation in", tblName)
				hasConstraintViolations = true
			}
		}
	}

	return hasConflicts, hasConstraintViolations
}

func printModifications(tblToStats map[string]*merge.MergeStats) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts == 0 && stats.ConstraintViolations == 0 {
			tbls = append(tbls, tblName)
			nameLen := len(tblName)
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.Conflicts

			if nameLen > maxNameLen {
				maxNameLen = nameLen
			}

			if modCount > maxModCount {
				maxModCount = modCount
			}

			rowsAdded += stats.Adds
			rowsChanged += stats.Modifications + stats.Conflicts
			rowsDeleted += stats.Deletes
		}
	}

	if len(tbls) == 0 {
		return
	}

	sort.Strings(tbls)
	modCountStrLen := len(strconv.FormatInt(int64(maxModCount), 10))
	format := fmt.Sprintf("%%-%ds | %%-%ds %%s", maxNameLen, modCountStrLen)

	for _, tbl := range tbls {
		stats := tblToStats[tbl]
		if stats.Operation == merge.TableModified {
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.Conflicts
			modCountStr := strconv.FormatInt(int64(modCount), 10)
			visualizedChanges := visualizeChangeTypes(stats, maxModCount)

			cli.Println(fmt.Sprintf(format, tbl, modCountStr, visualizedChanges))
		}
	}

	details := fmt.Sprintf("%d tables changed, %d rows added(+), %d rows modified(*), %d rows deleted(-)", len(tbls), rowsAdded, rowsChanged, rowsDeleted)
	cli.Println(details)
}

func visualizeChangeTypes(stats *merge.MergeStats, maxMods int) string {
	const maxVisLen = 30 //can be a bit longer due to min len and rounding

	resultStr := ""
	if stats.Adds > 0 {
		addLen := int(maxVisLen * (float64(stats.Adds) / float64(maxMods)))
		if addLen > stats.Adds {
			addLen = stats.Adds
		}
		addStr := fillStringWithChar('+', addLen)
		resultStr += color.GreenString(addStr)
	}

	if stats.Modifications > 0 {
		modLen := int(maxVisLen * (float64(stats.Modifications) / float64(maxMods)))
		if modLen > stats.Modifications {
			modLen = stats.Modifications
		}
		modStr := fillStringWithChar('*', modLen)
		resultStr += color.YellowString(modStr)
	}

	if stats.Deletes > 0 {
		delLen := int(maxVisLen * (float64(stats.Deletes) / float64(maxMods)))
		if delLen > stats.Deletes {
			delLen = stats.Deletes
		}
		delStr := fillStringWithChar('-', delLen)
		resultStr += color.GreenString(delStr)
	}

	return resultStr
}

func fillStringWithChar(ch rune, strLen int) string {
	if strLen == 0 {
		strLen = 1
	}

	runes := make([]rune, strLen)
	for i := 0; i < strLen; i++ {
		runes[i] = ch
	}

	return string(runes)
}

func ExecuteFFMerge(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergeCommit *doltdb.Commit,
	workingDiffs map[string]hash.Hash,
) errhand.VerboseError {
	cli.Println("Fast-forward")

	stagedRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
	}

	workingRoot := stagedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, workingDiffs)

		if err != nil {
			return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: unable to determine unstaged docs").AddCause(err).Build()
	}

	if !squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), mergeCommit)

		if err != nil {
			return errhand.BuildDError("Failed to write database").AddCause(err).Build()
		}
	}

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
	if err != nil {
		return errhand.BuildDError("unable to execute repo state update.").
			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
At the moment the best way to fix this is to run:

    dolt branch -v

and take the hash for your current branch and use it for the value for "staged" and "working"`).
			AddCause(err).Build()
	}

	err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
	}

	return nil
}

func ExecuteMerge(ctx context.Context, squash bool, dEnv *env.DoltEnv, cm1, cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) errhand.VerboseError {
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, cm1, cm2)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return errhand.BuildDError("Already up to date.").AddCause(err).Build()
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return errhand.BuildDError("Bad merge").AddCause(err).Build()
		}
	}

	return mergedRootToWorking(ctx, squash, dEnv, mergedRoot, workingDiffs, cm2, tblToStats)
}

// TODO: change this to be functional and not write to repo state
func mergedRootToWorking(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergedRoot *doltdb.RootValue,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	tblToStats map[string]*merge.MergeStats,
) errhand.VerboseError {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2)

		if err != nil {
			return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
		}
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
	}

	verr := UpdateWorkingWithVErr(dEnv, workingRoot)

	if verr == nil {
		hasConflicts, hasConstraintViolations := printSuccessStats(tblToStats)

		if hasConflicts && hasConstraintViolations {
			cli.Println("Automatic merge failed; fix conflicts and constraint violations and then commit the result.")
		} else if hasConflicts {
			cli.Println("Automatic merge failed; fix conflicts and then commit the result.")
		} else if hasConstraintViolations {
			cli.Println("Automatic merge failed; fix constraint violations and then commit the result.\n" +
				"Constraint violations for the working set may be viewed using the 'dolt_constraint_violations' system table.\n" +
				"They may be queried and removed per-table using the 'dolt_constraint_violations_TABLENAME' system table.")
		} else {
			err = SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
			if err != nil {
				return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
			}
			verr = UpdateStagedWithVErr(dEnv, mergedRoot)
			if verr != nil {
				// Log a new message here to indicate that merge was successful, only staging failed.
				cli.Println("Unable to stage changes: add and commit to finish merge")
			}
		}
	}

	return verr
}

func UpdateStagedWithVErr(doltEnv *env.DoltEnv, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	err := doltEnv.UpdateStagedRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the staged root state").Build()
	}

	return nil
}

func UpdateWorkingWithVErr(dEnv *env.DoltEnv, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	err := dEnv.UpdateWorkingRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the working root state").Build()
	}

	return nil
}
