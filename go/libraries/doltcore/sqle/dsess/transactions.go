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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	maxTxCommitRetries = 5
)

var ErrRetryTransaction = errors.New("this transaction conflicts with a committed transaction from another client")
var ErrUnresolvedConflictsCommit = errors.New("Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1")
var ErrUnresolvedConstraintViolationsCommit = errors.New("Committing this transaction resulted in a working set with constraint violations, transaction rolled back. " +
	"This constraint violation may be the result of a previous merge or the result of transaction sequencing. " +
	"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction. " +
	"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.")

func TransactionsDisabled(ctx *sql.Context) bool {
	enabled, err := ctx.GetSessionVariable(ctx, TransactionsDisabledSysVar)
	if err != nil {
		panic(err)
	}

	switch enabled.(int8) {
	case 0:
		return false
	case 1:
		return true
	default:
		panic(fmt.Sprintf("Unexpected value %v", enabled))
	}
}

// DisabledTransaction is a no-op transaction type that lets us feature-gate transaction logic changes
type DisabledTransaction struct{}

func (d DisabledTransaction) String() string {
	return "Disabled transaction"
}

func (d DisabledTransaction) IsReadOnly() bool {
	return false
}

type DoltTransaction struct {
	sourceDbName    string
	startState      *doltdb.WorkingSet
	workingSetRef   ref.WorkingSetRef
	dbData          env.DbData
	savepoints      []savepoint
	mergeEditOpts   editor.Options
	tCharacteristic sql.TransactionCharacteristic
}

type savepoint struct {
	name string
	root *doltdb.RootValue
}

func NewDoltTransaction(
	dbName string,
	startState *doltdb.WorkingSet,
	workingSet ref.WorkingSetRef,
	dbData env.DbData,
	mergeEditOpts editor.Options,
	tCharacteristic sql.TransactionCharacteristic,
) *DoltTransaction {
	return &DoltTransaction{
		sourceDbName:    dbName,
		startState:      startState,
		workingSetRef:   workingSet,
		dbData:          dbData,
		mergeEditOpts:   mergeEditOpts,
		tCharacteristic: tCharacteristic,
	}
}

func (tx DoltTransaction) String() string {
	// TODO: return more info (hashes need caching)
	return "DoltTransaction"
}

func (tx DoltTransaction) IsReadOnly() bool {
	return tx.tCharacteristic == sql.ReadOnly
}

var txLock sync.Mutex

// Commit attempts to merge the working set given into the current working set.
// Uses the same algorithm as merge.RootMerger:
// |current working set working root| is the root
// |workingSet.workingRoot| is the mergeRoot
// |tx.startRoot| is ancRoot
// if workingSet.workingRoot == ancRoot, attempt a fast-forward merge
// TODO: Non-working roots aren't merged into the working set and just stomp any changes made there. We need merge
// strategies for staged as well as merge state.
func (tx *DoltTransaction) Commit(ctx *sql.Context, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, error) {
	ws, _, err := tx.doCommit(ctx, workingSet, nil, txCommit)
	return ws, err
}

// transactionWrite is the logic to write an updated working set (and optionally a commit) to the database
type transactionWrite func(ctx *sql.Context,
	tx *DoltTransaction, // the transaction being written
	commit *doltdb.PendingCommit, // optional
	workingSet *doltdb.WorkingSet, // must be provided
	hash hash.Hash, // hash of the current working set to be written
) (*doltdb.WorkingSet, *doltdb.Commit, error)

// doltCommit is a transactionWrite function that updates the working set and commits a pending commit atomically
func doltCommit(ctx *sql.Context,
	tx *DoltTransaction,
	commit *doltdb.PendingCommit,
	workingSet *doltdb.WorkingSet,
	currHash hash.Hash,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	headRef, err := workingSet.Ref().ToHeadRef()
	if err != nil {
		return nil, nil, err
	}

	workingSet = workingSet.ClearMerge()
	newCommit, err := tx.dbData.Ddb.CommitWithWorkingSet(ctx, headRef, tx.workingSetRef, commit, workingSet, currHash, tx.getWorkingSetMeta(ctx))
	return workingSet, newCommit, err
}

// txCommit is a transactionWrite function that updates the working set
func txCommit(ctx *sql.Context,
	tx *DoltTransaction,
	_ *doltdb.PendingCommit,
	workingSet *doltdb.WorkingSet,
	hash hash.Hash,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	return workingSet, nil, tx.dbData.Ddb.UpdateWorkingSet(ctx, tx.workingSetRef, workingSet, hash, tx.getWorkingSetMeta(ctx))
}

// DoltCommit commits the working set and creates a new DoltCommit as specified, in one atomic write
func (tx *DoltTransaction) DoltCommit(ctx *sql.Context, workingSet *doltdb.WorkingSet, commit *doltdb.PendingCommit) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	return tx.doCommit(ctx, workingSet, commit, doltCommit)
}

// doCommit commits this transaction with the write function provided. It takes the same params as DoltCommit
func (tx *DoltTransaction) doCommit(
	ctx *sql.Context,
	workingSet *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	writeFn transactionWrite,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {

	for i := 0; i < maxTxCommitRetries; i++ {
		updatedWs, newCommit, err := func() (*doltdb.WorkingSet, *doltdb.Commit, error) {
			// Serialize commits, since only one can possibly succeed at a time anyway
			txLock.Lock()
			defer txLock.Unlock()

			newWorkingSet := false

			existingWs, err := tx.dbData.Ddb.ResolveWorkingSet(ctx, tx.workingSetRef)
			if err == doltdb.ErrWorkingSetNotFound {
				// This is to handle the case where an existing DB pre working sets is committing to this HEAD for the
				// first time. Can be removed and called an error post 1.0
				existingWs = doltdb.EmptyWorkingSet(tx.workingSetRef)
				newWorkingSet = true
			} else if err != nil {
				return nil, nil, err
			}

			existingWSHash, err := existingWs.HashOf()
			if err != nil {
				return nil, nil, err
			}

			if newWorkingSet || rootsEqual(existingWs.WorkingRoot(), tx.startState.WorkingRoot()) {
				// ff merge
				err = tx.validateWorkingSetForCommit(ctx, workingSet, isFfMerge)
				if err != nil {
					return nil, nil, err
				}

				var newCommit *doltdb.Commit
				workingSet, newCommit, err = writeFn(ctx, tx, commit, workingSet, existingWSHash)
				if err == datas.ErrOptimisticLockFailed {
					// this is effectively a `continue` in the loop
					return nil, nil, nil
				} else if err != nil {
					return nil, nil, err
				}

				return workingSet, newCommit, nil
			}

			// otherwise (not a ff), merge the working sets together
			start := time.Now()
			mergedWorkingSet, err := tx.mergeRoots(ctx, existingWs, workingSet)
			if err != nil {
				return nil, nil, err
			}
			logrus.Tracef("merge took %s", time.Since(start))

			err = tx.validateWorkingSetForCommit(ctx, mergedWorkingSet, notFfMerge)
			if err != nil {
				return nil, nil, err
			}

			var newCommit *doltdb.Commit
			mergedWorkingSet, newCommit, err = writeFn(ctx, tx, commit, mergedWorkingSet, existingWSHash)
			if err == datas.ErrOptimisticLockFailed {
				// this is effectively a `continue` in the loop
				return nil, nil, nil
			} else if err != nil {
				return nil, nil, err
			}

			return mergedWorkingSet, newCommit, nil
		}()

		if err != nil {
			return nil, nil, err
		} else if updatedWs != nil {
			return updatedWs, newCommit, nil
		}
	}

	// TODO: different error type for retries exhausted
	return nil, nil, datas.ErrOptimisticLockFailed
}

// mergeRoots merges the roots in the existing working set with the one being committed and returns the resulting
// working set. Conflicts are automatically resolved with "accept ours" if the session settings dictate it.
func (tx *DoltTransaction) mergeRoots(
	ctx *sql.Context,
	existingWorkingRoot *doltdb.WorkingSet,
	workingSet *doltdb.WorkingSet,
) (*doltdb.WorkingSet, error) {
	mo := merge.MergeOpts{IsCherryPick: false}
	mergedRoot, _, err := merge.MergeRoots(
		ctx,
		existingWorkingRoot.WorkingRoot(),
		workingSet.WorkingRoot(),
		tx.startState.WorkingRoot(),
		workingSet,
		tx.startState,
		tx.mergeEditOpts, mo)
	if err != nil {
		return nil, err
	}
	return workingSet.WithWorkingRoot(mergedRoot), nil
}

// rollback attempts a transaction rollback
func (tx *DoltTransaction) rollback(ctx *sql.Context) error {
	sess := DSessFromSess(ctx.Session)
	rollbackErr := sess.Rollback(ctx, tx)
	if rollbackErr != nil {
		return rollbackErr
	}

	// We also need to cancel out the transaction here so that a new one will begin on the next statement
	// TODO: it would be better for the engine to handle these details probably, this code is duplicated from the
	//  rollback statement implementation in the engine.
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)

	return nil
}

type ffMerge bool

const (
	isFfMerge  = ffMerge(true)
	notFfMerge = ffMerge(false)
)

// validateWorkingSetForCommit validates that the working set given is legal to
// commit according to the session settings. Returns an error if the given
// working set has conflicts or constraint violations and the session settings
// do not allow them.
//
// If dolt_allow_commit_conflicts = 0 and dolt_force_transaction_commit = 0, and
// a transaction's post-commit working set contains a documented conflict
// ( either as a result of a merge that occurred inside the transaction, or a
// result of a transaction merge) that transaction will be rolled back.
//
// The justification for this behavior is that we want to protect the working
// set from conflicts with the above settings.
//
// If dolt_force_transaction_commit = 0, and a transaction's post-commit working
// set contains a documented constraint violation ( either as a result of a merge
// that occurred inside the transaction, or a result of a transaction merge)
// that transaction will be rolled back.
//
// The justification for this behavior is that we want to protect the working
// set from constraint violations with the above settings.
func (tx *DoltTransaction) validateWorkingSetForCommit(ctx *sql.Context, workingSet *doltdb.WorkingSet, isFf ffMerge) error {
	forceTransactionCommit, err := ctx.GetSessionVariable(ctx, ForceTransactionCommit)
	if err != nil {
		return err
	}

	allowCommitConflicts, err := ctx.GetSessionVariable(ctx, AllowCommitConflicts)
	if err != nil {
		return err
	}

	workingRoot := workingSet.WorkingRoot()
	hasConflicts, err := workingRoot.HasConflicts(ctx)
	if err != nil {
		return err
	}
	hasConstraintViolations, err := workingRoot.HasConstraintViolations(ctx)
	if err != nil {
		return err
	}

	if hasConflicts {
		// TODO: Sometimes this returns the wrong error. Define an internal
		// merge to be a merge that occurs inside a transaction. Define a
		// transaction merge to be the merge that resolves changes between two
		// transactions. If an internal merge creates a documented conflict and
		// the transaction merge is not a fast-forward, a retry transaction
		// error will be returned. Instead, an ErrUnresolvedConflictsCommit should
		// be returned.

		// Conflicts are never acceptable when they resulted from a merge with the existing working set -- it's equivalent
		// to hitting a write lock (which we didn't take). Always roll back and return an error in this case.
		if !isFf {
			rollbackErr := tx.rollback(ctx)
			if rollbackErr != nil {
				return rollbackErr
			}

			return sql.ErrLockDeadlock.New(ErrRetryTransaction.Error())
		}

		// If there were conflicts before merge with the persisted working set, whether we allow it to be committed is a
		// session setting
		if !(allowCommitConflicts.(int8) == 1 || forceTransactionCommit.(int8) == 1) {
			rollbackErr := tx.rollback(ctx)
			if rollbackErr != nil {
				return rollbackErr
			}

			return ErrUnresolvedConflictsCommit
		}
	}

	if hasConstraintViolations {
		// Constraint violations are acceptable in the working set if force
		// transaction commit is enabled, regardless if an internal merge ( a
		// merge that occurs inside a transaction) or a transaction merge
		// created them.

		// TODO: We need to add more granularity in terms of what types of constraint violations can be committed. For example,
		// in the case of foreign_key_checks=0 you should be able to commit foreign key violations.
		if forceTransactionCommit.(int8) != 1 {
			rollbackErr := tx.rollback(ctx)
			if rollbackErr != nil {
				return rollbackErr
			}

			return ErrUnresolvedConstraintViolationsCommit
		}
	}

	return nil
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

func (tx DoltTransaction) getWorkingSetMeta(ctx *sql.Context) *datas.WorkingSetMeta {
	sess := DSessFromSess(ctx.Session)
	return &datas.WorkingSetMeta{
		Name:        sess.Username(),
		Email:       sess.Email(),
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
