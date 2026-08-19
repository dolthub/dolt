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

	errorKinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrBranchExists = errorKinds.NewKind("fatal: A branch named '%s' already exists.")
var ErrCOBranchDelete = errorKinds.NewKind("Cannot delete checked out branch '%s'")
var ErrUnmergedBranch = errorKinds.NewKind("branch '%s' is not fully merged")
var ErrWorkingSetsOnBothBranches = errors.New("checkout would overwrite uncommitted changes on target branch")

func RenameBranch[C doltdb.Context](ctx C, dbData env.DbData[C], oldBranch, newBranch string, force bool, rsc *doltdb.ReplicationStatusController) error {
	oldRef := ref.NewBranchRef(oldBranch)
	newRef := ref.NewBranchRef(newBranch)

	// Don't attempt to rename a branch to itself, otherwise removing oldBranch will remove the branch entirely.
	if oldBranch == newBranch {
		hasOld, err := dbData.Ddb.HasRef(ctx, oldRef)
		if err != nil {
			return err
		}
		if !hasOld {
			return doltdb.ErrBranchNotFound
		}
		return nil
	}

	// TODO: This function smears the branch updates across multiple commits of the datas.Database.

	// oldRef is exempt so a rename onto a case variant of its own name is allowed.
	err := CopyBranchOnDB(ctx, dbData.Ddb, oldBranch, newBranch, force, rsc, oldRef)
	if err != nil {
		return err
	}

	fromWSRef, err := ref.WorkingSetRefForHead(oldRef)
	if err != nil {
		if !errors.Is(err, ref.ErrWorkingSetUnsupported) {
			return err
		}
	} else {
		toWSRef, err := ref.WorkingSetRefForHead(newRef)
		if err != nil {
			return err
		}
		// We always `force` here, because the CopyBranch up
		// above created a new branch and it will have a
		// working set.
		err = dbData.Ddb.CopyWorkingSet(ctx, fromWSRef, toWSRef, true /* force */)
		if err != nil {
			return err
		}
	}

	// todo: update default branch variable

	return DeleteBranch(ctx, dbData, oldBranch, DeleteOptions{Force: true, AllowDeletingCurrentBranch: true}, rsc)
}

func CopyBranch(ctx context.Context, dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	return CopyBranchOnDB(ctx, dEnv.DoltDB(ctx), oldBranch, newBranch, force, nil)
}

// CopyBranchOnDB creates |newBranch| at the commit |oldBranch| points to.
//
// Without |force|, an existing |newBranch| is a doltdb.ExistingRefError.
// |except| an existing branch when a case conflict is intended, such as
// copying onto a different casing of its name.
func CopyBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, oldBranch, newBranch string, force bool, rsc *doltdb.ReplicationStatusController, except ...ref.DoltRef) error {
	oldRef := ref.NewBranchRef(oldBranch)
	newRef := ref.NewBranchRef(newBranch)

	hasOld, oldErr := ddb.HasRef(ctx, oldRef)

	if oldErr != nil {
		return oldErr
	}

	hasNew, newErr := ddb.HasRef(ctx, newRef)

	if newErr != nil {
		return newErr
	}

	if !hasOld {
		return doltdb.ErrBranchNotFound
	} else if !force && hasNew {
		return &doltdb.ExistingRefError{Ref: newRef}
	} else if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, _ := doltdb.NewCommitSpec(oldBranch)
	cm, err := ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	commit, ok := cm.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}
	return ddb.NewBranchAtCommit(ctx, newRef, commit, rsc, except...)
}

// BranchExistsError returns an ErrBranchExists naming the existing branch
// when |err| wraps an ExistingRefError, or nil otherwise.
func BranchExistsError(err error) error {
	if existing, ok := errors.AsType[*doltdb.ExistingRefError](err); ok {
		name := existing.Ref.GetPath()
		return ErrBranchExists.New(name)
	}
	return nil
}

type DeleteOptions struct {
	Force                      bool
	Remote                     bool
	AllowDeletingCurrentBranch bool
}

func DeleteBranch[C doltdb.Context](ctx C, dbData env.DbData[C], brName string, opts DeleteOptions, rsc *doltdb.ReplicationStatusController) error {
	var branchRef ref.DoltRef
	if opts.Remote {
		var err error
		branchRef, err = ref.NewRemoteRefFromPathStr(brName)
		if err != nil {
			return err
		}
	} else {
		branchRef = ref.NewBranchRef(brName)
		headRef, err := dbData.Rsr.CWBHeadRef(ctx)
		if err != nil {
			return err
		}
		if !opts.AllowDeletingCurrentBranch && ref.Equals(headRef, branchRef) {
			return ErrCOBranchDelete.New(brName)
		}
	}

	return DeleteBranchOnDB(ctx, dbData, branchRef, opts, rsc)
}

func DeleteBranchOnDB[C doltdb.Context](ctx C, dbdata env.DbData[C], branchRef ref.DoltRef, opts DeleteOptions, rsc *doltdb.ReplicationStatusController) error {
	ddb := dbdata.Ddb
	hasRef, err := ddb.HasRef(ctx, branchRef)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if !opts.Force && !opts.Remote {
		if err := validateBranchMerged(ctx, dbdata, branchRef); err != nil {
			return err
		}
	}

	wsRef, err := ref.WorkingSetRefForHead(branchRef)
	if err != nil {
		if !errors.Is(err, ref.ErrWorkingSetUnsupported) {
			return err
		}
	} else {
		err = ddb.DeleteWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}
	}

	return ddb.DeleteBranch(ctx, branchRef, rsc)
}

// validateBranchMerged checks that |branch|'s commits are contained
// by its upstream, or by the current working branch when |branch|
// has no upstream.
//
// It returns ErrUnmergedBranch when they are not.
func validateBranchMerged[C doltdb.Context](ctx C, dbdata env.DbData[C], branch ref.DoltRef) error {
	mergedInto, err := upstreamOrHead(ctx, dbdata, branch)
	if err != nil {
		return err
	}

	branchHead, err := dbdata.Ddb.ResolveCommitRef(ctx, branch)
	if err != nil {
		return err
	}

	merged, err := branchHead.CanFastForwardTo(ctx, mergedInto)
	if err != nil {
		if errors.Is(err, doltdb.ErrUpToDate) {
			return nil
		}
		if errors.Is(err, doltdb.ErrIsAhead) {
			return ErrUnmergedBranch.New(branch.GetPath())
		}
		return err
	}
	if !merged {
		return ErrUnmergedBranch.New(branch.GetPath())
	}
	return nil
}

// upstreamOrHead returns the commit at |branch|'s upstream, or the
// commit at the current working branch when that upstream does
// not resolve locally.
//
// A remote upstream is read from its local tracking ref, so the
// returned commit is only as current as the last fetch. See
// [git-branch] and [branch_merged].
//
// [git-branch]: https://git-scm.com/docs/git-branch#Documentation/git-branch.txt--d
// [branch_merged]: https://git.kernel.org/pub/scm/git/git.git/tree/builtin/branch.c?id=010afd3166ddc64c9863b1506f12cbcdda0d4ea1#n146
func upstreamOrHead[C doltdb.Context](ctx C, dbdata env.DbData[C], branch ref.DoltRef) (*doltdb.Commit, error) {
	upstream, err := env.UpstreamRef(dbdata.Rsr, branch)
	if err != nil {
		return nil, err
	}
	if upstream != nil {
		hasUpstream, err := dbdata.Ddb.HasRef(ctx, upstream)
		if err != nil {
			return nil, err
		}
		if hasUpstream {
			return dbdata.Ddb.ResolveCommitRef(ctx, upstream)
		}
	}

	headRef, err := dbdata.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return nil, err
	}
	return dbdata.Ddb.ResolveCommitRef(ctx, headRef)
}

func CreateBranchWithStartPt[C doltdb.Context](ctx C, dbData env.DbData[C], newBranch, startPt string, force bool, rsc *doltdb.ReplicationStatusController) error {
	err := createBranch(ctx, dbData, newBranch, startPt, force, rsc)

	if err != nil {
		if existsErr := BranchExistsError(err); existsErr != nil {
			return existsErr
		} else if err == doltdb.ErrInvBranchName {
			return fmt.Errorf("fatal: '%s' is an invalid branch name.", newBranch)
		} else if err == doltdb.ErrInvHash || doltdb.IsNotACommit(err) {
			return fmt.Errorf("fatal: '%s' is not a commit and a branch '%s' cannot be created from it", startPt, newBranch)
		} else {
			return fmt.Errorf("fatal: Unexpected error creating branch '%s' : %v", newBranch, err)
		}
	}
	err = branch_control.AddAdminForContext(ctx, newBranch)
	if err != nil {
		return err
	}

	return nil
}

func CreateBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, newBranch, startingPoint string, force bool, headRef ref.DoltRef, rsc *doltdb.ReplicationStatusController) error {
	branchRef := ref.NewBranchRef(newBranch)
	hasRef, err := ddb.HasRef(ctx, branchRef)
	if err != nil {
		return err
	}

	if !force && hasRef {
		return &doltdb.ExistingRefError{Ref: branchRef}
	}

	if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, err := doltdb.NewCommitSpec(startingPoint)
	if err != nil {
		return err
	}

	optCmt, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}

	cm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	return ddb.NewBranchAtCommit(ctx, branchRef, cm, rsc)
}

func createBranch[C doltdb.Context](ctx C, dbData env.DbData[C], newBranch, startingPoint string, force bool, rsc *doltdb.ReplicationStatusController) error {
	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return err
	}
	return CreateBranchOnDB(ctx, dbData.Ddb, newBranch, startingPoint, force, headRef, rsc)
}

var emptyHash = hash.Hash{}

func IsBranch(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	dref := ref.NewBranchRef(str)
	return ddb.HasRef(ctx, dref)
}

func IsTag(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	tRef := ref.NewTagRef(str)
	return ddb.HasRef(ctx, tRef)
}

func MaybeGetCommit(ctx context.Context, dEnv *env.DoltEnv, str string) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(str)

	if err == nil {
		headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
		if err != nil {
			return nil, err
		}
		optCmt, err := dEnv.DoltDB(ctx).Resolve(ctx, cs, headRef)
		if err != nil && errors.Is(err, doltdb.ErrBranchNotFound) {
			return nil, nil
		}
		if err != nil && errors.Is(err, doltdb.ErrHashNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		cm, ok := optCmt.ToCommit()
		if ok {
			return cm, nil
		}
	}

	return nil, nil
}
