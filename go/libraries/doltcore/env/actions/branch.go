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
var ErrUnmergedBranchDelete = errors.New("attempted to delete a branch that is not fully merged into master; use `-f` to force")

func MoveBranch(ctx context.Context, dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	oldRef := ref.NewBranchRef(oldBranch)
	newRef := ref.NewBranchRef(newBranch)

	err := CopyBranch(ctx, dEnv, oldBranch, newBranch, force)

	if err != nil {
		return err
	}

	if ref.Equals(dEnv.RepoState.CWBHeadRef(), oldRef) {
		dEnv.RepoState.Head = ref.MarshalableRef{Ref: newRef}
		err = dEnv.RepoState.Save(dEnv.FS)

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
		if ref.Equals(dEnv.RepoState.CWBHeadRef(), dref) {
			return ErrCOBranchDelete
		}
	}

	return DeleteBranchOnDB(ctx, dEnv.DoltDB, dref, opts)
}

func DeleteBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, dref ref.DoltRef, opts DeleteOptions) error {
	hasRef, err := ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if !opts.Force && !opts.Remote {
		ms, err := doltdb.NewCommitSpec("master")
		if err != nil {
			return err
		}

		master, err := ddb.Resolve(ctx, ms, nil)
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

		isMerged, _ := master.CanFastReverseTo(ctx, cm)
		if err != nil && err != doltdb.ErrUpToDate {
			return err
		}
		if !isMerged {
			return ErrUnmergedBranchDelete
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
	newRef := ref.NewBranchRef(newBranch)
	hasRef, err := ddb.HasRef(ctx, newRef)

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

	return ddb.NewBranchAtCommit(ctx, newRef, cm)
}

func createBranch(ctx context.Context, dbData env.DbData, newBranch, startingPoint string, force bool) error {
	return CreateBranchOnDB(ctx, dbData.Ddb, newBranch, startingPoint, force, dbData.Rsr.CWBHeadRef())
}

// updateRootsForBranch writes the roots needed for a checkout and returns the updated work and staged hash.
func updateRootsForBranch(ctx context.Context, dbData env.DbData, dref ref.DoltRef, brName string) (wrkHash hash.Hash, stgHash hash.Hash, err error) {
	hasRef, err := dbData.Ddb.HasRef(ctx, dref)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}
	if !hasRef {
		return hash.Hash{}, hash.Hash{}, doltdb.ErrBranchNotFound
	}
	if ref.Equals(dbData.Rsr.CWBHeadRef(), dref) {
		return hash.Hash{}, hash.Hash{}, doltdb.ErrAlreadyOnBranch
	}

	currRoots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, doltdb.HeadRoot, doltdb.WorkingRoot, doltdb.StagedRoot)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}

	cs, err := doltdb.NewCommitSpec(brName)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	cm, err := dbData.Ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	newRoot, err := cm.GetRootValue()
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}

	conflicts := set.NewStrSet([]string{})

	wrkTblHashes, err := moveModifiedTables(ctx, currRoots[doltdb.HeadRoot], newRoot, currRoots[doltdb.WorkingRoot], conflicts)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}

	stgTblHashes, err := moveModifiedTables(ctx, currRoots[doltdb.HeadRoot], newRoot, currRoots[doltdb.StagedRoot], conflicts)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}
	if conflicts.Size() > 0 {
		return hash.Hash{}, hash.Hash{}, CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	wrkHash, err = writeRoot(ctx, dbData.Ddb, newRoot, wrkTblHashes)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}

	stgHash, err = writeRoot(ctx, dbData.Ddb, newRoot, stgTblHashes)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}

	return wrkHash, stgHash, nil
}

func CheckoutBranch(ctx context.Context, dEnv *env.DoltEnv, brName string) error {
	dbData := dEnv.DbData()
	dref := ref.NewBranchRef(brName)

	wrkHash, stgHash, err := updateRootsForBranch(ctx, dbData, dref, brName)
	if err != nil {
		return err
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dbData)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetWorkingHash(ctx, wrkHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(ctx, stgHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: dref})
	if err != nil {
		return err
	}

	return SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
}

// CheckoutBranchWithoutDocs checkouts a branch without considering any working changes to the local docs. Used
// with DOLT_CHECKOUT.
func CheckoutBranchWithoutDocs(ctx context.Context, dbData env.DbData, brName string) error {
	dref := ref.NewBranchRef(brName)

	wrkHash, stgHash, err := updateRootsForBranch(ctx, dbData, dref, brName)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetWorkingHash(ctx, wrkHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(ctx, stgHash)
	if err != nil {
		return err
	}

	return dbData.Rsw.SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: dref})
}

var emptyHash = hash.Hash{}

// moveModifiedTables handles working set changes during a branch change.
// When moving between branches, changes in the working set should travel with you.
// Working set changes cannot be moved if the table differs between the old and new head,
// in this case, we throw a conflict and error (as per Git).
func moveModifiedTables(ctx context.Context, oldRoot, newRoot, changedRoot *doltdb.RootValue, conflicts *set.StrSet) (map[string]hash.Hash, error) {
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
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}
	}

	return resultMap, nil
}

func writeRoot(ctx context.Context, ddb *doltdb.DoltDB, head *doltdb.RootValue, tblHashes map[string]hash.Hash) (hash.Hash, error) {
	names, err := head.GetTableNames(ctx)
	if err != nil {
		return hash.Hash{}, err
	}

	var toDrop []string
	for _, name := range names {
		if _, ok := tblHashes[name]; !ok {
			toDrop = append(toDrop, name)
		}
	}

	head, err = head.RemoveTables(ctx, toDrop...)
	if err != nil {
		return hash.Hash{}, err
	}

	for k, v := range tblHashes {
		if v == emptyHash {
			continue
		}

		head, err = head.SetTableHash(ctx, k, v)
		if err != nil {
			return hash.Hash{}, err
		}
	}

	return ddb.WriteRootValue(ctx, head)
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
		cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())

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
