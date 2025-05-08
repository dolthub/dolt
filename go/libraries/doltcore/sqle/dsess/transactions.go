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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

const (
	maxTxCommitRetries = 5
)

var ErrRetryTransaction = errors.New("this transaction conflicts with a committed transaction from another client")

var ErrUnresolvedConflictsCommit = errors.New("Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts and dolt_schema_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1")

var ErrUnresolvedConflictsAutoCommit = errors.New("Merge conflict detected, @autocommit transaction rolled back. @autocommit must be disabled so that merge conflicts can be resolved using the dolt_conflicts and dolt_schema_conflicts tables before manually committing the transaction. Alternatively, to commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1")

var ErrUnresolvedConstraintViolationsCommit = errors.New("Committing this transaction resulted in a working set with constraint violations, transaction rolled back. " +
	"This constraint violation may be the result of a previous merge or the result of transaction sequencing. " +
	"Constraint violations from a merge can be resolved using the dolt_constraint_violations table before committing the transaction. " +
	"To allow transactions to be committed with constraint violations from a merge or transaction sequencing set @@dolt_force_transaction_commit=1.")

// TODO: remove this
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
	dbStartPoints   map[string]dbRoot
	savepoints      []savepoint
	tCharacteristic sql.TransactionCharacteristic
}

type dbRoot struct {
	dbName   string
	rootHash hash.Hash
	db       *doltdb.DoltDB
}

type savepoint struct {
	name string
	// from db name to the root value for that database.
	roots map[string]doltdb.RootValue
}

func NewDoltTransaction(
	ctx *sql.Context,
	dbs []SqlDatabase,
	tCharacteristic sql.TransactionCharacteristic,
) (*DoltTransaction, error) {

	startPoints := make(map[string]dbRoot)
	for _, db := range dbs {
		nomsRoot, err := db.DbData().Ddb.NomsRoot(ctx)
		if err != nil {
			return nil, err
		}

		baseName, _ := SplitRevisionDbName(db.Name())
		startPoints[strings.ToLower(baseName)] = dbRoot{
			dbName:   baseName,
			rootHash: nomsRoot,
			db:       db.DbData().Ddb,
		}
	}

	return &DoltTransaction{
		dbStartPoints:   startPoints,
		tCharacteristic: tCharacteristic,
	}, nil
}

// AddDb adds the database named to the transaction. Only necessary in the case when new databases are added to an
// existing transaction (as when cloning a database on a read replica when it is first referenced).
func (tx DoltTransaction) AddDb(ctx *sql.Context, db SqlDatabase) error {
	nomsRoot, err := db.DbData().Ddb.NomsRoot(ctx)
	if err != nil {
		return err
	}

	tx.dbStartPoints[strings.ToLower(db.Name())] = dbRoot{
		dbName:   db.Name(),
		rootHash: nomsRoot,
		db:       db.DbData().Ddb,
	}

	return nil
}

func (tx DoltTransaction) String() string {
	// TODO: return more info (hashes need caching)
	return "DoltTransaction"
}

func (tx DoltTransaction) IsReadOnly() bool {
	return tx.tCharacteristic == sql.ReadOnly
}

// GetInitialRoot returns the noms root hash for the db named, established when the transaction began. The dbName here
// is always the base name of the database, not the revision qualified one.
func (tx DoltTransaction) GetInitialRoot(dbName string) (hash.Hash, bool) {
	dbName, _ = SplitRevisionDbName(dbName)
	startPoint, ok := tx.dbStartPoints[strings.ToLower(dbName)]
	return startPoint.rootHash, ok
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
func (tx *DoltTransaction) Commit(ctx *sql.Context, workingSet *doltdb.WorkingSet, dbName string) (*doltdb.WorkingSet, error) {
	ws, _, err := tx.doCommit(ctx, workingSet, nil, txCommit, dbName)
	return ws, err
}

// transactionWrite is the logic to write an updated working set (and optionally a commit) to the database
type transactionWrite func(ctx *sql.Context,
	tx *DoltTransaction, // the transaction being written
	doltDb *doltdb.DoltDB, // the database to write to
	startState *doltdb.WorkingSet, // the starting working set
	commit *doltdb.PendingCommit, // optional
	workingSet *doltdb.WorkingSet, // must be provided
	hash hash.Hash, // hash of the current working set to be written
	mergeOps editor.Options, // editor options for merges
) (*doltdb.WorkingSet, *doltdb.Commit, error)

// doltCommit is a transactionWrite function that updates the working set and commits a pending commit atomically
func doltCommit(ctx *sql.Context,
	tx *DoltTransaction, // the transaction being written
	doltDb *doltdb.DoltDB, // the database to write to
	startState *doltdb.WorkingSet, // the starting working set
	commit *doltdb.PendingCommit, // optional
	workingSet *doltdb.WorkingSet, // must be provided
	currHash hash.Hash, // hash of the current working set to be written
	mergeOpts editor.Options, // editor options for merges
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	pending := *commit

	headRef, err := workingSet.Ref().ToHeadRef()
	if err != nil {
		return nil, nil, err
	}

	headSpec, _ := doltdb.NewCommitSpec("HEAD")
	optCmt, err := doltDb.Resolve(ctx, headSpec, headRef)
	if err != nil {
		return nil, nil, err
	}
	curHead, ok := optCmt.ToCommit()
	if !ok {
		return nil, nil, doltdb.ErrGhostCommitRuntimeFailure
	}

	// We already got a new staged root via merge or ff via the doCommit method, so now apply it to the STAGED value
	// we're about to commit.
	pending.Roots.Staged = workingSet.StagedRoot()

	// We check if the branch HEAD has changed since our transaction started and perform an additional merge if so. The
	// non-dolt-commit transaction logic only merges working sets and doesn't consider the HEAD value.
	if curHead != nil {
		curRootVal, err := curHead.ResolveRootValue(ctx)
		if err != nil {
			return nil, nil, err
		}
		curRootValHash, err := curRootVal.HashOf()
		if err != nil {
			return nil, nil, err
		}
		headRootValHash, err := pending.Roots.Head.HashOf()
		if err != nil {
			return nil, nil, err
		}

		if curRootValHash != headRootValHash {
			// If the branch head changed since our transaction started, then we merge
			// the existing branch head (curRootVal) into our staged root value. We
			// treat the HEAD of the branch when our transaction started as the common
			// ancestor (TODO: This will not be true in the case of destructive branch
			// updates). The merged root value becomes our new Staged root value which
			// is the value which we are trying to commit.
			start := time.Now()

			result, err := merge.MergeRoots(
				ctx,
				pending.Roots.Staged,
				curRootVal,
				pending.Roots.Head,
				curHead,
				startState,
				mergeOpts,
				merge.MergeOpts{})
			if err != nil {
				return nil, nil, err
			}
			pending.Roots.Staged = result.Root

			// We also need to update the working set to reflect the new staged root value
			workingSet = workingSet.WithStagedRoot(pending.Roots.Staged)

			logrus.Tracef("staged and HEAD merge took %s", time.Since(start))
		}
	}

	workingSet = workingSet.ClearMerge()

	var rsc doltdb.ReplicationStatusController
	newCommit, err := doltDb.CommitWithWorkingSet(ctx, headRef, workingSet.Ref(), &pending, workingSet, currHash, tx.WorkingSetMeta(ctx), &rsc)
	WaitForReplicationController(ctx, rsc)
	return workingSet, newCommit, err
}

// txCommit is a transactionWrite function that updates the working set
func txCommit(ctx *sql.Context,
	tx *DoltTransaction, // the transaction being written
	doltDb *doltdb.DoltDB, // the database to write to
	_ *doltdb.WorkingSet, // the starting working set
	_ *doltdb.PendingCommit, // optional
	workingSet *doltdb.WorkingSet, // must be provided
	hash hash.Hash, // hash of the current working set to be written
	_ editor.Options, // editor options for merges
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	var rsc doltdb.ReplicationStatusController
	err := doltDb.UpdateWorkingSet(ctx, workingSet.Ref(), workingSet, hash, tx.WorkingSetMeta(ctx), &rsc)
	WaitForReplicationController(ctx, rsc)
	return workingSet, nil, err
}

// DoltCommit commits the working set and creates a new DoltCommit as specified, in one atomic write
func (tx *DoltTransaction) DoltCommit(
	ctx *sql.Context,
	workingSet *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	dbName string,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	return tx.doCommit(ctx, workingSet, commit, doltCommit, dbName)
}

func WaitForReplicationController(ctx *sql.Context, rsc doltdb.ReplicationStatusController) {
	if len(rsc.Wait) == 0 {
		return
	}
	_, timeout, ok := sql.SystemVariables.GetGlobal(DoltClusterAckWritesTimeoutSecs)
	if !ok {
		return
	}
	timeoutI := timeout.(int64)
	if timeoutI == 0 {
		return
	}

	cCtx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	wg.Add(len(rsc.Wait))
	for i, f := range rsc.Wait {
		f := f
		i := i
		go func() {
			defer wg.Done()
			err := f(cCtx)
			if err == nil {
				rsc.Wait[i] = nil
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	waitFailed := false
	select {
	case <-time.After(time.Duration(timeoutI) * time.Second):
		// We timed out before all the waiters were done.
		// First we make certain to finalize everything.
		cancel(doltdb.ErrReplicationWaitFailed)
		<-done
		waitFailed = true
	case <-done:
		cancel(context.Canceled)
	}

	// Just because our waiters all completed does not mean they all
	// returned nil errors. Any non-nil entries in rsc.Wait returned an
	// error. We turn those into warnings here.
	numFailed := 0
	for i, f := range rsc.Wait {
		if f != nil {
			numFailed += 1
			if waitFailed {
				rsc.NotifyWaitFailed[i]()
			}
		}
	}
	if numFailed > 0 {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Warning",
			Code:    mysql.ERQueryTimeout,
			Message: fmt.Sprintf("Timed out replication of commit to %d out of %d replicas.", numFailed, len(rsc.Wait)),
		})
	}
}

// doCommit commits this transaction with the write function provided. It takes the same params as DoltCommit
func (tx *DoltTransaction) doCommit(
	ctx *sql.Context,
	workingSet *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	writeFn transactionWrite,
	dbName string,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	sess := DSessFromSess(ctx.Session)
	branchState, ok, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("database %s unknown to transaction, this is a bug", dbName)
	}

	// Load the start state for this working set from the noms root at tx start
	// Get the base DB name from the db state, not the branch state
	startPoint, ok := tx.dbStartPoints[strings.ToLower(branchState.dbState.dbName)]
	if !ok {
		return nil, nil, fmt.Errorf("database %s unknown to transaction, this is a bug", dbName)
	}

	startState, err := startPoint.db.ResolveWorkingSetAtRoot(ctx, workingSet.Ref(), startPoint.rootHash)
	if err != nil {
		return nil, nil, err
	}

	// TODO: no-op if the working set hasn't changed since the transaction started

	mergeOpts := branchState.EditOpts()

	for i := 0; i < maxTxCommitRetries; i++ {
		updatedWs, newCommit, err := func() (*doltdb.WorkingSet, *doltdb.Commit, error) {
			// Serialize commits, since only one can possibly succeed at a time anyway
			txLock.Lock()
			defer txLock.Unlock()

			newWorkingSet := false

			existingWs, err := startPoint.db.ResolveWorkingSet(ctx, workingSet.Ref())
			if err == doltdb.ErrWorkingSetNotFound {
				// This is to handle the case where this is the first commit to a branch which
				// does not have a working set. Typically Dolt creates a working set when it
				// creates the branch. However, things like pushing a branch to a remote do not
				// typically eagerly create a working set which does not exist. Since sql-server
				// can run as a doltremoteapi remote endpoint and accept writes, this logic
				// should anti-entropy the lack of a working set here.
				existingWs = doltdb.EmptyWorkingSet(workingSet.Ref())
				newWorkingSet = true
			} else if err != nil {
				return nil, nil, err
			}

			existingWSHash, err := existingWs.HashOf()
			if err != nil {
				return nil, nil, err
			}

			if newWorkingSet || workingAndStagedEqual(existingWs, startState) {
				// ff merge
				err = tx.validateWorkingSetForCommit(ctx, workingSet, isFfMerge)
				if err != nil {
					return nil, nil, err
				}

				var newCommit *doltdb.Commit
				workingSet, newCommit, err = writeFn(ctx, tx, startPoint.db, startState, commit, workingSet, existingWSHash, mergeOpts)
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
			mergedWorkingSet, err := tx.mergeRoots(ctx, startState, existingWs, workingSet, mergeOpts)
			if err != nil {
				return nil, nil, err
			}
			logrus.Tracef("working set merge took %s", time.Since(start))

			err = tx.validateWorkingSetForCommit(ctx, mergedWorkingSet, notFfMerge)
			if err != nil {
				return nil, nil, err
			}

			var newCommit *doltdb.Commit
			mergedWorkingSet, newCommit, err = writeFn(ctx, tx, startPoint.db, startState, commit, mergedWorkingSet, existingWSHash, mergeOpts)
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
// Currently merges working and staged roots as necessary. HEAD root is only handled by the DoltCommit function.
func (tx *DoltTransaction) mergeRoots(
	ctx *sql.Context,
	startState *doltdb.WorkingSet,
	existingWorkingSet *doltdb.WorkingSet,
	workingSet *doltdb.WorkingSet,
	mergeOpts editor.Options,
) (*doltdb.WorkingSet, error) {

	if !rootsEqual(existingWorkingSet.WorkingRoot(), workingSet.WorkingRoot()) {
		result, err := merge.MergeRoots(
			ctx,
			existingWorkingSet.WorkingRoot(),
			workingSet.WorkingRoot(),
			startState.WorkingRoot(),
			workingSet,
			startState,
			mergeOpts,
			merge.MergeOpts{})
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithWorkingRoot(result.Root)
	}

	if !rootsEqual(existingWorkingSet.StagedRoot(), workingSet.StagedRoot()) {
		result, err := merge.MergeRoots(
			ctx,
			existingWorkingSet.StagedRoot(),
			workingSet.StagedRoot(),
			startState.StagedRoot(),
			workingSet,
			startState,
			mergeOpts,
			merge.MergeOpts{})
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithStagedRoot(result.Root)
	}

	return workingSet, nil
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
// TODO: should this validate staged as well?
func (tx *DoltTransaction) validateWorkingSetForCommit(ctx *sql.Context, workingSet *doltdb.WorkingSet, isFf ffMerge) error {
	forceTransactionCommit, err := ctx.GetSessionVariable(ctx, ForceTransactionCommit)
	if err != nil {
		return err
	}

	allowCommitConflicts, err := ctx.GetSessionVariable(ctx, AllowCommitConflicts)
	if err != nil {
		return err
	}

	hasSchemaConflicts := false
	if workingSet.MergeState() != nil {
		hasSchemaConflicts = workingSet.MergeState().HasSchemaConflicts()
	}

	workingRoot := workingSet.WorkingRoot()
	hasDataConflicts, err := doltdb.HasConflicts(ctx, workingRoot)
	if err != nil {
		return err
	}
	hasConstraintViolations, err := doltdb.HasConstraintViolations(ctx, workingRoot)
	if err != nil {
		return err
	}

	if hasDataConflicts || hasSchemaConflicts {
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

			// Return a different error message depending on if @autocommit is enabled or not, to help
			// users understand what steps to take
			autocommit, err := isSessionAutocommit(ctx)
			if err != nil {
				return err
			}
			if autocommit {
				return ErrUnresolvedConflictsAutoCommit
			} else {
				return ErrUnresolvedConflictsCommit
			}
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
			badTbls, err := doltdb.TablesWithConstraintViolations(ctx, workingRoot)
			if err != nil {
				return err
			}

			violations := make([]string, len(badTbls))
			for i, name := range badTbls {
				tbl, _, err := workingRoot.GetTable(ctx, name)
				if err != nil {
					return err
				}

				artIdx, err := tbl.GetArtifacts(ctx)
				if err != nil {
					return err
				}

				m := durable.ProllyMapFromArtifactIndex(artIdx)
				itr, err := m.IterAllCVs(ctx)

				for {
					art, err := itr.Next(ctx)
					if err != nil {
						break
					}

					var meta prolly.ConstraintViolationMeta
					err = json.Unmarshal(art.Metadata, &meta)
					if err != nil {
						return err
					}

					s := ""
					switch art.ArtType {
					case prolly.ArtifactTypeForeignKeyViol:
						var m merge.FkCVMeta
						err = json.Unmarshal(meta.VInfo, &m)
						if err != nil {
							return err
						}
						s = fmt.Sprintf("\n"+
							"Type: Foreign Key Constraint Violation\n"+
							"\tForeignKey: %s,\n"+
							"\tTable: %s,\n"+
							"\tReferencedTable: %s,\n"+
							"\tIndex: %s,\n"+
							"\tReferencedIndex: %s", m.ForeignKey, m.Table, m.ReferencedIndex, m.Index, m.ReferencedIndex)

					case prolly.ArtifactTypeUniqueKeyViol:
						var m merge.UniqCVMeta
						err = json.Unmarshal(meta.VInfo, &m)
						if err != nil {
							return err
						}
						s = fmt.Sprintf("\n"+
							"Type: Unique Key Constraint Violation,\n"+
							"\tName: %s,\n"+
							"\tColumns: %v", m.Name, m.Columns)

					case prolly.ArtifactTypeNullViol:
						var m merge.NullViolationMeta
						err = json.Unmarshal(meta.VInfo, &m)
						if err != nil {
							return err
						}
						s = fmt.Sprintf("\n"+
							"Type: Null Constraint Violation,\n"+
							"\tColumns: %v", m.Columns)

					case prolly.ArtifactTypeChkConsViol:
						var m merge.CheckCVMeta
						err = json.Unmarshal(meta.VInfo, &m)
						if err != nil {
							return err
						}
						s = fmt.Sprintf("\n"+
							"Type: Check Constraint Violation,\n"+
							"\tName: %s,\n"+
							"\tExpression: %v", m.Name, m.Expression)
					}
					if err != nil {
						return err
					}

					violations[i] = s
				}
			}

			rollbackErr := tx.rollback(ctx)
			if rollbackErr != nil {
				return rollbackErr
			}

			return fmt.Errorf("%s\n"+
				"Constraint violations: %s", ErrUnresolvedConstraintViolationsCommit, strings.Join(violations, ", "))
		}
	}

	return nil
}

// CreateSavepoint creates a new savepoint with the name and roots given. If a savepoint with the name given
// already exists, it's overwritten.
func (tx *DoltTransaction) CreateSavepoint(name string, roots map[string]doltdb.RootValue) {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		tx.savepoints = append(tx.savepoints[:existing], tx.savepoints[existing+1:]...)
	}
	tx.savepoints = append(tx.savepoints, savepoint{name, roots})
}

// findSavepoint returns the index of the savepoint with the name given, or -1 if it doesn't exist
func (tx *DoltTransaction) findSavepoint(name string) int {
	for i, s := range tx.savepoints {
		if strings.EqualFold(s.name, name) {
			return i
		}
	}
	return -1
}

// RollbackToSavepoint returns the root values for all applicable databases associated with the savepoint name given, or nil if no such savepoint can
// be found. All savepoints created after the one being rolled back to are no longer accessible.
func (tx *DoltTransaction) RollbackToSavepoint(name string) map[string]doltdb.RootValue {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		// Clear out any savepoints past this one
		tx.savepoints = tx.savepoints[:existing+1]
		return tx.savepoints[existing].roots
	}
	return nil
}

// ClearSavepoint removes the savepoint with the name given and returns whether a savepoint had that name
func (tx *DoltTransaction) ClearSavepoint(name string) bool {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		tx.savepoints = append(tx.savepoints[:existing], tx.savepoints[existing+1:]...)
		return true
	}
	return false
}

// WorkingSetMeta returns the metadata to use for a commit of this transaction
func (tx DoltTransaction) WorkingSetMeta(ctx *sql.Context) *datas.WorkingSetMeta {
	sess := DSessFromSess(ctx.Session)
	return &datas.WorkingSetMeta{
		Name:        sess.Username(),
		Email:       sess.Email(),
		Timestamp:   uint64(time.Now().Unix()),
		Description: "sql transaction",
	}
}

func rootsEqual(left, right doltdb.RootValue) bool {
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

func workingAndStagedEqual(left, right *doltdb.WorkingSet) bool {
	return rootsEqual(left.WorkingRoot(), right.WorkingRoot()) && rootsEqual(left.StagedRoot(), right.StagedRoot())
}

// isSessionAutocommit returns true if @autocommit is enabled.
func isSessionAutocommit(ctx *sql.Context) (bool, error) {
	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return sql.ConvertToBool(ctx, autoCommitSessionVar)
}
