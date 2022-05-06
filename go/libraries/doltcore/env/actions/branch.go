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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrAlreadyExists = errors.New("already exists")
var ErrCOBranchDelete = errors.New("attempted to delete checked out branch")
var ErrUnmergedBranchDelete = errors.New("attempted to delete a branch that is not fully merged into its parent; use `-f` to force")

func RenameBranch(ctx context.Context, dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	oldRef := ref.NewBranchRef(oldBranch)
	newRef := ref.NewBranchRef(newBranch)

	err := CopyBranch(ctx, dEnv, oldBranch, newBranch, force)
	if err != nil {
		return err
	}

	if ref.Equals(dEnv.RepoStateReader().CWBHeadRef(), oldRef) {
		err = dEnv.RepoStateWriter().SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: newRef})
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
		err = dEnv.DoltDB.CopyWorkingSet(ctx, fromWSRef, toWSRef, true /* force */)
		if err != nil {
			return err
		}
	}

	return DeleteBranch(ctx, dEnv, oldBranch, DeleteOptions{Force: true})
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

func DeleteBranch(ctx context.Context, dEnv *env.DoltEnv, brName string, opts DeleteOptions) error {
	var dref ref.DoltRef
	if opts.Remote {
		var err error
		dref, err = ref.NewRemoteRefFromPathStr(brName)
		if err != nil {
			return err
		}
	} else {
		dref = ref.NewBranchRef(brName)
		if ref.Equals(dEnv.RepoStateReader().CWBHeadRef(), dref) {
			return ErrCOBranchDelete
		}
	}

	return DeleteBranchOnDB(ctx, dEnv, dref, opts)
}

func DeleteBranchOnDB(ctx context.Context, dEnv *env.DoltEnv, dref ref.DoltRef, opts DeleteOptions) error {
	ddb := dEnv.DoltDB
	hasRef, err := ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if !opts.Force && !opts.Remote {
		ms, err := doltdb.NewCommitSpec(env.GetDefaultInitBranch(dEnv.Config))
		if err != nil {
			return err
		}

		init, err := ddb.Resolve(ctx, ms, nil)
		if err != nil {
			return err
		}

		cs, err := doltdb.NewCommitSpec(dref.String())
		if err != nil {
			return err
		}

		cm, err := ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}

		isMerged, _ := init.CanFastReverseTo(ctx, cm)
		if err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
			return err
		}
		if !isMerged {
			return ErrUnmergedBranchDelete
		}
	}

	wsRef, err := ref.WorkingSetRefForHead(dref)
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

	return ddb.DeleteBranch(ctx, dref)
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

func CheckoutBranchNoDocs(ctx context.Context, roots doltdb.Roots, branchRoot *doltdb.RootValue, rsw env.RepoStateWriter, branchRef ref.BranchRef, force bool) error {
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

	roots, err := dEnv.Roots(ctx)
	if errors.Is(err, doltdb.ErrBranchNotFound) {
		roots, err = dEnv.RecoveryRoots(ctx)
	}
	if err != nil {
		return err
	}

	unstagedDocs, err := GetUnstagedDocsFromRoots(ctx, dEnv, roots)
	if err != nil {
		return err
	}

	err = CheckoutBranchNoDocs(ctx, roots, branchRoot, dEnv.RepoStateWriter(), branchRef, force)
	if err != nil {
		return err
	}

	return SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
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
