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

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrAlreadyExists = errors.New("already exists")
var ErrCOBranchDelete = errors.New("attempted to delete checked out branch")
var ErrUnmergedBranch = errors.New("branch is not fully merged")
var ErrWorkingSetsOnBothBranches = errors.New("checkout would overwrite uncommitted changes on target branch")

func RenameBranch(ctx context.Context, dbData env.DbData, oldBranch, newBranch string, remoteDbPro env.RemoteDbProvider, force bool) error {
	oldRef := ref.NewBranchRef(oldBranch)
	newRef := ref.NewBranchRef(newBranch)

	err := CopyBranchOnDB(ctx, dbData.Ddb, oldBranch, newBranch, force)
	if err != nil {
		return err
	}

	if ref.Equals(dbData.Rsr.CWBHeadRef(), oldRef) {
		err = dbData.Rsw.SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: newRef})
		if err != nil {
			return err
		}
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

	return DeleteBranch(ctx, dbData, oldBranch, DeleteOptions{Force: true}, remoteDbPro)
}

func CopyBranch(ctx context.Context, dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	return CopyBranchOnDB(ctx, dEnv.DoltDB, oldBranch, newBranch, force)
}

func CopyBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, oldBranch, newBranch string, force bool) error {
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
		return ErrAlreadyExists
	} else if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, _ := doltdb.NewCommitSpec(oldBranch)
	cm, err := ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return err
	}

	return ddb.NewBranchAtCommit(ctx, newRef, cm)
}

type DeleteOptions struct {
	Force  bool
	Remote bool
}

func DeleteBranch(ctx context.Context, dbData env.DbData, brName string, opts DeleteOptions, remoteDbPro env.RemoteDbProvider) error {
	var branchRef ref.DoltRef
	if opts.Remote {
		var err error
		branchRef, err = ref.NewRemoteRefFromPathStr(brName)
		if err != nil {
			return err
		}
	} else {
		branchRef = ref.NewBranchRef(brName)
		if ref.Equals(dbData.Rsr.CWBHeadRef(), branchRef) {
			return ErrCOBranchDelete
		}
	}

	return DeleteBranchOnDB(ctx, dbData, branchRef, opts, remoteDbPro)
}

func DeleteBranchOnDB(ctx context.Context, dbdata env.DbData, branchRef ref.DoltRef, opts DeleteOptions, pro env.RemoteDbProvider) error {
	ddb := dbdata.Ddb
	hasRef, err := ddb.HasRef(ctx, branchRef)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if !opts.Force && !opts.Remote {
		// check to see if the branch is fully merged into its parent
		trackedBranches, err := dbdata.Rsr.GetBranches()
		if err != nil {
			return err
		}

		trackedBranch, hasUpstream := trackedBranches[branchRef.GetPath()]
		if hasUpstream {
			err = validateBranchMergedIntoUpstream(ctx, dbdata, branchRef, trackedBranch.Remote, pro)
			if err != nil {
				return err
			}
		} else {
			err = validateBranchMergedIntoCurrentWorkingBranch(ctx, dbdata, branchRef)
			if err != nil {
				return err
			}
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

	return ddb.DeleteBranch(ctx, branchRef)
}

// validateBranchMergedIntoCurrentWorkingBranch returns an error if the given branch is not fully merged into the HEAD of the current branch.
func validateBranchMergedIntoCurrentWorkingBranch(ctx context.Context, dbdata env.DbData, branch ref.DoltRef) error {
	branchSpec, err := doltdb.NewCommitSpec(branch.GetPath())
	if err != nil {
		return err
	}

	branchHead, err := dbdata.Ddb.Resolve(ctx, branchSpec, nil)
	if err != nil {
		return err
	}

	cwbCs, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return err
	}

	cwbHead, err := dbdata.Ddb.Resolve(ctx, cwbCs, dbdata.Rsr.CWBHeadRef())
	if err != nil {
		return err
	}

	isMerged, err := branchHead.CanFastForwardTo(ctx, cwbHead)
	if err != nil {
		if errors.Is(err, doltdb.ErrUpToDate) {
			return nil
		}
		if errors.Is(err, doltdb.ErrIsAhead) {
			return ErrUnmergedBranch
		}

		return err
	}

	if !isMerged {
		return ErrUnmergedBranch
	}

	return nil
}

// validateBranchMergedIntoUpstream returns an error if the branch provided is not fully merged into its upstream
func validateBranchMergedIntoUpstream(ctx context.Context, dbdata env.DbData, branch ref.DoltRef, remoteName string, pro env.RemoteDbProvider) error {
	remotes, err := dbdata.Rsr.GetRemotes()
	if err != nil {
		return err
	}
	remote, ok := remotes[remoteName]
	if !ok {
		// TODO: skip error?
		return fmt.Errorf("remote %s not found", remoteName)
	}

	remoteDb, err := pro.GetRemoteDB(ctx, dbdata.Ddb.ValueReadWriter().Format(), remote, false)
	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec(branch.GetPath())
	if err != nil {
		return err
	}

	remoteBranchHead, err := remoteDb.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	localBranchHead, err := dbdata.Ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	canFF, err := localBranchHead.CanFastForwardTo(ctx, remoteBranchHead)
	if err != nil {
		if errors.Is(err, doltdb.ErrUpToDate) {
			return nil
		}
		if errors.Is(err, doltdb.ErrIsAhead) {
			return ErrUnmergedBranch
		}
		return err
	}

	if !canFF {
		return ErrUnmergedBranch
	}

	return nil
}

func CreateBranchWithStartPt(ctx context.Context, dbData env.DbData, newBranch, startPt string, force bool) error {
	err := createBranch(ctx, dbData, newBranch, startPt, force)

	if err != nil {
		if err == ErrAlreadyExists {
			return fmt.Errorf("fatal: A branch named '%s' already exists.", newBranch)
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

func CreateBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, newBranch, startingPoint string, force bool, headRef ref.DoltRef) error {
	branchRef := ref.NewBranchRef(newBranch)
	hasRef, err := ddb.HasRef(ctx, branchRef)
	if err != nil {
		return err
	}

	if !force && hasRef {
		return ErrAlreadyExists
	}

	if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, err := doltdb.NewCommitSpec(startingPoint)
	if err != nil {
		return err
	}

	cm, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}

	err = ddb.NewBranchAtCommit(ctx, branchRef, cm)
	if err != nil {
		return err
	}

	return nil
}

func createBranch(ctx context.Context, dbData env.DbData, newBranch, startingPoint string, force bool) error {
	return CreateBranchOnDB(ctx, dbData.Ddb, newBranch, startingPoint, force, dbData.Rsr.CWBHeadRef())
}

// UpdateRootsForBranch writes the roots needed for a branch checkout and returns the updated roots. |roots.Head|
// should be the pre-checkout head. The returned roots struct has |Head| set to |branchRoot|.
func UpdateRootsForBranch(ctx context.Context, roots doltdb.Roots, branchRoot *doltdb.RootValue, force bool) (doltdb.Roots, error) {
	conflicts := set.NewStrSet([]string{})
	if roots.Head == nil {
		roots.Working = branchRoot
		roots.Staged = branchRoot
		roots.Head = branchRoot
		return roots, nil
	}

	wrkTblHashes, err := moveModifiedTables(ctx, roots.Head, branchRoot, roots.Working, conflicts, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	stgTblHashes, err := moveModifiedTables(ctx, roots.Head, branchRoot, roots.Staged, conflicts, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	if conflicts.Size() > 0 {
		return doltdb.Roots{}, CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	roots.Working, err = overwriteRoot(ctx, branchRoot, wrkTblHashes)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = overwriteRoot(ctx, branchRoot, stgTblHashes)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Head = branchRoot
	return roots, nil
}

func checkoutBranchNoDocs(ctx context.Context, roots doltdb.Roots, branchRoot *doltdb.RootValue, rsw env.RepoStateWriter, branchRef ref.BranchRef, force bool) error {
	roots, err := UpdateRootsForBranch(ctx, roots, branchRoot, force)
	if err != nil {
		return err
	}

	err = rsw.SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: branchRef})
	if err != nil {
		return err
	}

	// TODO: combine into single update
	err = rsw.UpdateWorkingRoot(ctx, roots.Working)
	if err != nil {
		return err
	}

	return rsw.UpdateStagedRoot(ctx, roots.Staged)
}

func CheckoutBranch(ctx context.Context, dEnv *env.DoltEnv, brName string, force bool) error {
	branchRef := ref.NewBranchRef(brName)
	branchHeadRef := dEnv.RepoStateReader().CWBHeadRef()

	db := dEnv.DoltDB
	hasRef, err := db.HasRef(ctx, branchRef)
	if err != nil {
		return err
	}
	if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if ref.Equals(dEnv.RepoStateReader().CWBHeadRef(), branchRef) {
		return doltdb.ErrAlreadyOnBranch
	}

	branchRoot, err := BranchRoot(ctx, db, brName)
	if err != nil {
		return err
	}

	currentWs, err := dEnv.WorkingSet(ctx)
	if err != nil {
		// working set does not exist, skip check
		return nil
	}

	if !force {
		err = checkWorkingSetCompatibility(ctx, dEnv, branchRef, currentWs)
		if err != nil {
			return err
		}
	}

	shouldResetWorkingSet := true
	roots, err := dEnv.Roots(ctx)
	// roots will be empty/nil if the working set is not set (working set is not set if the current branch was deleted)
	if errors.Is(err, doltdb.ErrBranchNotFound) || errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		roots, _ = dEnv.RecoveryRoots(ctx)
		shouldResetWorkingSet = false
	} else if err != nil {
		return err
	}

	err = checkoutBranchNoDocs(ctx, roots, branchRoot, dEnv.RepoStateWriter(), branchRef, force)
	if err != nil {
		return err
	}

	if shouldResetWorkingSet {
		// reset the source branch's working set to the branch head, leaving the source branch unchanged
		err = ResetHard(ctx, dEnv, "", roots, branchHeadRef, currentWs)
		if err != nil {
			return err
		}
	}

	return nil
}

// BranchRoot returns the root value at the branch with the name given
// TODO: this belongs in DoltDB, maybe
func BranchRoot(ctx context.Context, db *doltdb.DoltDB, brName string) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(brName)
	if err != nil {
		return nil, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	cm, err := db.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	branchRoot, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	return branchRoot, nil
}

var emptyHash = hash.Hash{}

// moveModifiedTables handles working set changes during a branch change.
// When moving between branches, changes in the working set should travel with you.
// Working set changes cannot be moved if the table differs between the old and new head,
// in this case, we throw a conflict and error (as per Git).
func moveModifiedTables(ctx context.Context, oldRoot, newRoot, changedRoot *doltdb.RootValue, conflicts *set.StrSet, force bool) (map[string]hash.Hash, error) {
	resultMap := make(map[string]hash.Hash)
	tblNames, err := newRoot.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	for _, tblName := range tblNames {
		oldHash, _, err := oldRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		newHash, _, err := newRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		changedHash, _, err := changedRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		if oldHash == changedHash {
			resultMap[tblName] = newHash
		} else if oldHash == newHash {
			resultMap[tblName] = changedHash
		} else if newHash == changedHash {
			resultMap[tblName] = oldHash
		} else if force {
			resultMap[tblName] = newHash
		} else {
			conflicts.Add(tblName)
		}
	}

	tblNames, err = changedRoot.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	for _, tblName := range tblNames {
		if _, exists := resultMap[tblName]; !exists {
			oldHash, _, err := oldRoot.GetTableHash(ctx, tblName)
			if err != nil {
				return nil, err
			}

			changedHash, _, err := changedRoot.GetTableHash(ctx, tblName)
			if err != nil {
				return nil, err
			}

			if oldHash == emptyHash {
				resultMap[tblName] = changedHash
			} else if force {
				resultMap[tblName] = oldHash
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}
	}

	return resultMap, nil
}

// overwriteRoot writes new table hash values for the root given and returns it.
// This is an inexpensive and convenient way of replacing all the tables at once.
func overwriteRoot(ctx context.Context, head *doltdb.RootValue, tblHashes map[string]hash.Hash) (*doltdb.RootValue, error) {
	names, err := head.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var toDrop []string
	for _, name := range names {
		if _, ok := tblHashes[name]; !ok {
			toDrop = append(toDrop, name)
		}
	}

	head, err = head.RemoveTables(ctx, false, false, toDrop...)
	if err != nil {
		return nil, err
	}

	for k, v := range tblHashes {
		if v == emptyHash {
			continue
		}

		head, err = head.SetTableHash(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	return head, nil
}

// checkWorkingSetCompatibility checks that the current working set is "compatible" with the dest working set.
// This means that if both working sets are present (ie there are changes on both source and dest branches),
// we check if the changes are identical before allowing a clobbering checkout.
// Working set errors are ignored by this function, because they are properly handled elsewhere.
func checkWorkingSetCompatibility(ctx context.Context, dEnv *env.DoltEnv, branchRef ref.BranchRef, currentWs *doltdb.WorkingSet) error {
	db := dEnv.DoltDB
	destWsRef, err := ref.WorkingSetRefForHead(branchRef)
	if err != nil {
		// dest working set does not exist, skip check
		return nil
	}
	destWs, err := db.ResolveWorkingSet(ctx, destWsRef)
	if err != nil {
		// dest working set does not resolve, skip check
		return nil
	}

	sourceHasChanges, sourceHash, err := detectWorkingSetChanges(currentWs)
	if err != nil {
		// error detecting source changes, skip check
		return nil
	}
	destHasChanges, destHash, err := detectWorkingSetChanges(destWs)
	if err != nil {
		// error detecting dest changes, skip check
		return nil
	}
	areHashesEqual := sourceHash.Equal(destHash)

	if sourceHasChanges && destHasChanges && !areHashesEqual {
		return ErrWorkingSetsOnBothBranches
	}
	return nil
}

// detectWorkingSetChanges returns a boolean indicating whether the working set has changes, and a hash of the changes
func detectWorkingSetChanges(ws *doltdb.WorkingSet) (hasChanges bool, wrHash hash.Hash, err error) {
	wrHash, err = ws.WorkingRoot().HashOf()
	if err != nil {
		return false, hash.Hash{}, err
	}
	srHash, err := ws.StagedRoot().HashOf()
	if err != nil {
		return false, hash.Hash{}, err
	}
	hasChanges = !wrHash.Equal(srHash)
	return hasChanges, wrHash, nil
}

func IsBranch(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	return IsBranchOnDB(ctx, ddb, str)
}

func IsBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	dref := ref.NewBranchRef(str)
	return ddb.HasRef(ctx, dref)
}

func MaybeGetCommit(ctx context.Context, dEnv *env.DoltEnv, str string) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(str)

	if err == nil {
		cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())

		if errors.Is(err, doltdb.ErrBranchNotFound) {
			return nil, nil
		}

		switch err {
		case nil:
			return cm, nil

		case doltdb.ErrHashNotFound, doltdb.ErrBranchNotFound:
			return nil, nil

		default:
			return nil, err
		}
	}

	return nil, nil
}
