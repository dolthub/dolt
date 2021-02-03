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
	"github.com/dolthub/dolt/go/store/types"
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

func CheckoutBranch(ctx context.Context, dbData env.DbData, brName string) error {
	dref := ref.NewBranchRef(brName)

	hasRef, err := dbData.Ddb.HasRef(ctx, dref)
	if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if ref.Equals(dbData.Rsr.CWBHeadRef(), dref) {
		return doltdb.ErrAlreadyOnBranch
	}

	currRoots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, HeadRoot, WorkingRoot, StagedRoot)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec(brName)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	cm, err := dbData.Ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	newRoot, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	ssMap, err := newRoot.GetSuperSchemaMap(ctx)

	if err != nil {
		return err
	}

	fkMap, err := newRoot.GetForeignKeyCollectionMap(ctx)

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

	wrkHash, err := writeRoot(ctx, dbData.Ddb, wrkTblHashes, ssMap, fkMap)

	if err != nil {
		return err
	}

	stgHash, err := writeRoot(ctx, dbData.Ddb, stgTblHashes, ssMap, fkMap)

	if err != nil {
		return err
	}

	err = dbData.Rsw.SetWorkingHash(wrkHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(stgHash)
	if err != nil {
		return err
	}

	return dbData.Rsw.SetCWBHeadRef(ref.MarshalableRef{Ref: dref})
}

func CheckoutUnstagedDocs(ctx context.Context, dEnv *env.DoltEnv) error {
	dbData := dEnv.DbData()

	unstagedDocs, err := GetUnstagedDocs(ctx, dbData)
	if err != nil {
		return err
	}

	return SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
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

func writeRoot(ctx context.Context, ddb *doltdb.DoltDB, tblHashes map[string]hash.Hash, ssMap types.Map, fkMap types.Map) (hash.Hash, error) {
	for k, v := range tblHashes {
		if v == emptyHash {
			delete(tblHashes, k)
		}
	}

	root, err := doltdb.NewRootValue(ctx, ddb.ValueReadWriter(), tblHashes, ssMap, fkMap)
	if err != nil {
		if err == doltdb.ErrHashNotFound {
			return emptyHash, errors.New("corrupted database? Can't find hash of current table")
		}
		return emptyHash, doltdb.ErrNomsIO
	}

	return ddb.WriteRootValue(ctx, root)

}

func RootsWithTable(ctx context.Context, dEnv *env.DoltEnv, table string) (RootTypeSet, error) {
	roots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), ActiveRoots...)

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
