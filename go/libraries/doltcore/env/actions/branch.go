// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/hash"
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

	if ref.Equals(dEnv.RepoState.Head.Ref, oldRef) {
		dEnv.RepoState.Head = ref.MarshalableRef{Ref: newRef}
		err = dEnv.RepoState.Save(dEnv.FS)

		if err != nil {
			return err
		}
	}

	return DeleteBranch(ctx, dEnv, oldBranch, true)
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

	cs, _ := doltdb.NewCommitSpec("head", oldBranch)
	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return err
	}

	return ddb.NewBranchAtCommit(ctx, newRef, cm)
}

func DeleteBranch(ctx context.Context, dEnv *env.DoltEnv, brName string, force bool) error {
	dref := ref.NewBranchRef(brName)

	if ref.Equals(dEnv.RepoState.Head.Ref, dref) {
		return ErrCOBranchDelete
	}

	return DeleteBranchOnDB(ctx, dEnv.DoltDB, dref, force)
}

func DeleteBranchOnDB(ctx context.Context, ddb *doltdb.DoltDB, dref ref.DoltRef, force bool) error {
	hasRef, err := ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	ms, err := doltdb.NewCommitSpec("head", "master")

	if err != nil {
		return err
	}

	master, err := ddb.Resolve(ctx, ms)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec("head", dref.String())

	if err != nil {
		return err
	}

	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return err
	}

	if !force {
		if isMerged, _ := master.CanFastReverseTo(ctx, cm); !isMerged {
			return ErrUnmergedBranchDelete
		}
	}

	return ddb.DeleteBranch(ctx, dref)
}

func CreateBranch(ctx context.Context, dEnv *env.DoltEnv, newBranch, startingPoint string, force bool) error {
	newRef := ref.NewBranchRef(newBranch)

	hasRef, err := dEnv.DoltDB.HasRef(ctx, newRef)

	if err != nil {
		return err
	}

	if !force && hasRef {
		return ErrAlreadyExists
	}

	if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, err := doltdb.NewCommitSpec(startingPoint, dEnv.RepoState.Head.Ref.String())

	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewBranchAtCommit(ctx, newRef, cm)
}

func CheckoutBranch(ctx context.Context, dEnv *env.DoltEnv, brName string) error {
	dref := ref.NewBranchRef(brName)

	hasRef, err := dEnv.DoltDB.HasRef(ctx, dref)
	if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if ref.Equals(dEnv.RepoState.Head.Ref, dref) {
		return doltdb.ErrAlreadyOnBranch
	}

	currRoots, err := getRoots(ctx, dEnv, HeadRoot, WorkingRoot, StagedRoot)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec("head", brName)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	newRoot, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	conflicts := set.NewStrSet([]string{})
	wrkTblHashes, err := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[WorkingRoot], conflicts)

	if err != nil {
		return err
	}

	stgTblHashes, err := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[StagedRoot], conflicts)

	if err != nil {
		return err
	}

	if conflicts.Size() > 0 {
		return CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	wrkHash, err := writeRoot(ctx, dEnv, wrkTblHashes)

	if err != nil {
		return err
	}

	stgHash, err := writeRoot(ctx, dEnv, stgTblHashes)

	if err != nil {
		return err
	}

	dEnv.RepoState.Head = ref.MarshalableRef{Ref: dref}
	dEnv.RepoState.Working = wrkHash.String()
	dEnv.RepoState.Staged = stgHash.String()

	err = dEnv.RepoState.Save(dEnv.FS)

	if err != nil {
		return err
	}

	return SaveTrackedDocsFromWorking(ctx, dEnv)
}

var emptyHash = hash.Hash{}

func tblHashesForCO(ctx context.Context, oldRoot, newRoot, changedRoot *doltdb.RootValue, conflicts *set.StrSet) (map[string]hash.Hash, error) {
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

func writeRoot(ctx context.Context, dEnv *env.DoltEnv, tblHashes map[string]hash.Hash) (hash.Hash, error) {
	for k, v := range tblHashes {
		if v == emptyHash {
			delete(tblHashes, k)
		}
	}

	root, err := doltdb.NewRootValue(ctx, dEnv.DoltDB.ValueReadWriter(), tblHashes)
	if err != nil {
		if err == doltdb.ErrHashNotFound {
			return emptyHash, errors.New("corrupted database? Can't find hash of current table")
		}
		return emptyHash, doltdb.ErrNomsIO
	}

	return dEnv.DoltDB.WriteRootValue(ctx, root)

}

func RootsWithTable(ctx context.Context, dEnv *env.DoltEnv, table string) (RootTypeSet, error) {
	roots, err := getRoots(ctx, dEnv, ActiveRoots...)

	if err != nil {
		return nil, err
	}

	rootsWithTable := make([]RootType, 0, len(roots))

	for rt, root := range roots {
		if has, err := root.HasTable(ctx, table); err != nil {
			return nil, err
		} else if has {
			rootsWithTable = append(rootsWithTable, rt)
		}
	}

	return NewRootTypeSet(rootsWithTable...), nil
}

func IsBranch(ctx context.Context, dEnv *env.DoltEnv, str string) (bool, error) {
	dref := ref.NewBranchRef(str)
	return dEnv.DoltDB.HasRef(ctx, dref)
}

func MaybeGetCommit(ctx context.Context, dEnv *env.DoltEnv, str string) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(str, dEnv.RepoState.Head.Ref.String())

	if err == nil {
		cm, err := dEnv.DoltDB.Resolve(ctx, cs)

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
