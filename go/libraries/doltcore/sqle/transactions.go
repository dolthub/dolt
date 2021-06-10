// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	maxTxCommitRetries = 5
)

type DoltTransaction struct {
	startRoot  *doltdb.RootValue
	workingSet ref.WorkingSetRef
	dbData     env.DbData
	savepoints []savepoint
}

type savepoint struct {
	name string
	root *doltdb.RootValue
}

func NewDoltTransaction(startRoot *doltdb.RootValue, workingSet ref.WorkingSetRef, dbData env.DbData) *DoltTransaction {
	return &DoltTransaction{
		startRoot:  startRoot,
		workingSet: workingSet,
		dbData:     dbData,
	}
}

func (tx DoltTransaction) String() string {
	// TODO: return more info (hashes need caching)
	return "DoltTransaction"
}

// Commit attempts to merge newRoot into the working set
// Uses the same algorithm as merge.Merger:
// |ws.root| is the root
// |newRoot| is the mergeRoot
// |tx.startRoot| is ancRoot
// if working set == ancRoot, attempt a fast-forward merge
func (tx *DoltTransaction) Commit(ctx *sql.Context, newRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	for i := 0; i < maxTxCommitRetries; i++ {
		ws, err := tx.dbData.Ddb.ResolveWorkingSet(ctx, tx.workingSet)
		if err == doltdb.ErrWorkingSetNotFound {
			// initial commit
			err = tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSet, newRoot, hash.Hash{})
			if err == datas.ErrOptimisticLockFailed {
				continue
			}
		}

		if err != nil {
			return nil, err
		}

		root := ws.RootValue()

		hash, err := ws.HashOf()
		if err != nil {
			return nil, err
		}

		if rootsEqual(root, tx.startRoot) {
			// ff merge
			err = tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSet, newRoot, hash)
			if err == datas.ErrOptimisticLockFailed {
				continue
			} else if err != nil {
				return nil, err
			}

			return tx.updateRepoStateFile(ctx, newRoot)
		}

		mergedRoot, stats, err := merge.MergeRoots(ctx, root, newRoot, tx.startRoot)
		if err != nil {
			return nil, err
		}

		for table, mergeStats := range stats {
			if mergeStats.Conflicts > 0 {
				// TODO: surface duplicate key errors as appropriate
				return nil, fmt.Errorf("conflict in table %s", table)
			}
		}

		err = tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSet, mergedRoot, hash)
		if err == datas.ErrOptimisticLockFailed {
			continue
		} else if err != nil {
			return nil, err
		}

		// TODO: this is not thread safe, but will not be necessary after migrating all clients away from using the
		//  working set stored in repo_state.json, so should be good enough for now
		return tx.updateRepoStateFile(ctx, mergedRoot)
	}

	// TODO: different error type for retries exhausted
	return nil, datas.ErrOptimisticLockFailed
}

func (tx *DoltTransaction) updateRepoStateFile(ctx *sql.Context, mergedRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	hash, err := mergedRoot.HashOf()
	if err != nil {
		return nil, err
	}

	err = tx.dbData.Rsw.SetWorkingHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	return mergedRoot, err
}

// CreateSavepoint creates a new savepoint with the name and root value given. If a savepoint with the name given
// already exists, it's overwritten.
func (tx *DoltTransaction) CreateSavepoint(name string, root *doltdb.RootValue) {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		tx.savepoints = append(tx.savepoints[:existing], tx.savepoints[existing+1:]...)
	}
	tx.savepoints = append(tx.savepoints, savepoint{name, root})
}

// findSavepoint returns the index of the savepoint with the name given, or -1 if it doesn't exist
func (tx *DoltTransaction) findSavepoint(name string) int {
	for i, s := range tx.savepoints {
		if strings.ToLower(s.name) == strings.ToLower(name) {
			return i
		}
	}
	return -1
}

// RollbackToSavepoint returns the root value associated with the savepoint name given, or nil if no such savepoint can
// be found. All savepoints created after the one being rolled back to are no longer accessible.
func (tx *DoltTransaction) RollbackToSavepoint(name string) *doltdb.RootValue {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		// Clear out any savepoints past this one
		tx.savepoints = tx.savepoints[:existing+1]
		return tx.savepoints[existing].root
	}
	return nil
}

// ClearSavepoint removes the savepoint with the name given and returns the root value recorded there, or nil if no
// savepoint exists with that name.
func (tx *DoltTransaction) ClearSavepoint(name string) *doltdb.RootValue {
	existing := tx.findSavepoint(name)
	var existingRoot *doltdb.RootValue
	if existing >= 0 {
		existingRoot = tx.savepoints[existing].root
		tx.savepoints = append(tx.savepoints[:existing], tx.savepoints[existing+1:]...)
	}
	return existingRoot
}

func rootsEqual(left, right *doltdb.RootValue) bool {
	if left == nil || right == nil {
		return false
	}

	lh, err := left.HashOf()
	if err != nil {
		return false
	}

	rh, err := right.HashOf()
	if err != nil {
		return false
	}

	return lh == rh
}
