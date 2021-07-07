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

package dsess

import (
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
)

const (
	maxTxCommitRetries = 5
)

type DoltTransaction struct {
	startState    *doltdb.WorkingSet
	workingSetRef ref.WorkingSetRef
	dbData        env.DbData
	savepoints    []savepoint
}

type savepoint struct {
	name string
	root *doltdb.RootValue
}

func NewDoltTransaction(startState *doltdb.WorkingSet, workingSet ref.WorkingSetRef, dbData env.DbData) *DoltTransaction {
	return &DoltTransaction{
		startState:    startState,
		workingSetRef: workingSet,
		dbData:        dbData,
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
// TODO: Non-working roots aren't merged into the working set and just stomp any changes made there. We need merge
//  strategies for staged as well as merge state.
func (tx *DoltTransaction) Commit(ctx *sql.Context, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, error) {
	// logrus.Errorf("Committing working root %s", workingSet.WorkingRoot().DebugString(ctx, true))

	// Don't allow a root value with conflicts to be committed. Later we may open this up via configuration
	hasConflicts, err := workingSet.WorkingRoot().HasConflicts(ctx)
	if err != nil {
		return nil, err
	}

	if hasConflicts {
		return nil, doltdb.ErrUnresolvedConflicts
	}

	for i := 0; i < maxTxCommitRetries; i++ {
		newWorkingSet := false

		ws, err := tx.dbData.Ddb.ResolveWorkingSet(ctx, tx.workingSetRef)
		if err == doltdb.ErrWorkingSetNotFound {
			// This is to handle the case where an existing DB pre working sets is committing to this HEAD for the
			// first time. Can be removed and called an error post 1.0
			ws = doltdb.EmptyWorkingSet(tx.workingSetRef)
			newWorkingSet = true
		} else if err != nil {
			return nil, err
		}

		existingWorkingRoot := ws.RootValue()

		hash, err := ws.HashOf()
		if err != nil {
			return nil, err
		}

		if newWorkingSet || rootsEqual(existingWorkingRoot, tx.startState.WorkingRoot()) {
			// ff merge
			err = tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSetRef, workingSet, hash, tx.getWorkingSetMeta(ctx))
			if err == datas.ErrOptimisticLockFailed {
				continue
			} else if err != nil {
				return nil, err
			}

			return workingSet, nil
		}

		mergedRoot, stats, err := merge.MergeRoots(ctx, existingWorkingRoot, workingSet.WorkingRoot(), tx.startState.WorkingRoot())
		if err != nil {
			return nil, err
		}

		for table, mergeStats := range stats {
			if mergeStats.Conflicts > 0 {
				// TODO: surface duplicate key errors as appropriate
				return nil, fmt.Errorf("conflict in table %s", table)
			}
		}

		mergedWorkingSet := workingSet.WithWorkingRoot(mergedRoot)
		err = tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSetRef, mergedWorkingSet, hash, tx.getWorkingSetMeta(ctx))
		if err == datas.ErrOptimisticLockFailed {
			continue
		} else if err != nil {
			return nil, err
		}

		return mergedWorkingSet, nil
	}

	// TODO: different error type for retries exhausted
	return nil, datas.ErrOptimisticLockFailed
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

func (tx DoltTransaction) getWorkingSetMeta(ctx *sql.Context) *doltdb.WorkingSetMeta {
	sess := DSessFromSess(ctx.Session)
	return &doltdb.WorkingSetMeta{
		User:        sess.Username,
		Email:       sess.Email,
		Timestamp:   uint64(time.Now().Unix()),
		Description: "sql transaction",
	}
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
