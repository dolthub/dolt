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
	"sort"
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

// ConstraintViolationsListPrefix is the label appended before the violation list in commit errors.
const ConstraintViolationsListPrefix = "\nConstraint violations: "

type DoltTransaction struct {
	dbStartPoints   map[string]dbRoot
	savepoints      []savepoint
	tCharacteristic sql.TransactionCharacteristic
}

type dbRoot struct {
	db       *doltdb.DoltDB
	dbName   string
	rootHash hash.Hash
}

type savepoint struct {
	roots         map[string]doltdb.RootValue
	name          string
	dirtyBranches map[string]bool
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

		baseName, _ := doltdb.SplitRevisionDbName(db.Name())
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

// AddDb adds the database named to the transaction, establishing a start-point root for it. Necessary when a database
// becomes visible to a session after its transaction has already begun: when cloning a database on a read replica as it
// is first referenced, or when another session created the database concurrently after this transaction's snapshot was
// taken. The key is normalized to the base (non-revision-qualified, lowercased) name to match NewDoltTransaction and
// GetInitialRoot, since db.Name() returns the user-requested name, which may be revision-qualified or differently cased.
func (tx *DoltTransaction) AddDb(ctx *sql.Context, db SqlDatabase) error {
	nomsRoot, err := db.DbData().Ddb.NomsRoot(ctx)
	if err != nil {
		return err
	}

	baseName, _ := doltdb.SplitRevisionDbName(db.Name())
	tx.dbStartPoints[strings.ToLower(baseName)] = dbRoot{
		dbName:   baseName,
		rootHash: nomsRoot,
		db:       db.DbData().Ddb,
	}

	return nil
}

func (tx *DoltTransaction) String() string {
	// TODO: return more info (hashes need caching)
	return "DoltTransaction"
}

func (tx *DoltTransaction) IsReadOnly() bool {
	return tx.tCharacteristic == sql.ReadOnly
}

// GetInitialRoot returns the noms root hash for the db named, established when the transaction began. The dbName here
// is always the base name of the database, not the revision qualified one.
func (tx *DoltTransaction) GetInitialRoot(dbName string) (hash.Hash, bool) {
	dbName, _ = doltdb.SplitRevisionDbName(dbName)
	startPoint, ok := tx.dbStartPoints[strings.ToLower(dbName)]
	return startPoint.rootHash, ok
}

// CommitWorkingSet attempts to merge the working set given into the current working set.
// Uses the same algorithm as merge.RootMerger:
// |current working set working root| is the root
// |workingSet.workingRoot| is the mergeRoot
// |tx.startRoot| is ancRoot
// if workingSet.workingRoot == ancRoot, attempt a fast-forward merge
// TODO: Non-working roots aren't merged into the working set and just stomp any changes made there. We need merge
// strategies for staged as well as merge state.
func (tx *DoltTransaction) CommitWorkingSet(ctx *sql.Context, workingSet *doltdb.WorkingSet, dbName string) (*doltdb.WorkingSet, error) {
	ws, _, err := tx.doCommit(ctx, workingSet, nil, dbName)
	return ws, err
}

// prepareDoltCommit merges a pending commit with the current HEAD without publishing it.
func prepareDoltCommit(ctx *sql.Context,
	dbName string,
	doltDb *doltdb.DoltDB,
	startState *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	workingSet *doltdb.WorkingSet,
	mergeOpts editor.Options,
) (*doltdb.WorkingSet, *doltdb.PendingCommit, error) {
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

			tableResolver, err := GetTableResolver(ctx, dbName)
			if err != nil {
				return nil, nil, err
			}
			result, err := merge.MergeRoots(
				ctx,
				tableResolver,
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

	return workingSet, &pending, nil
}

// DoltCommit commits the working set and creates a new DoltCommit as specified, in one atomic write
func (tx *DoltTransaction) DoltCommit(
	ctx *sql.Context,
	workingSet *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	dbName string,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	return tx.doCommit(ctx, workingSet, commit, dbName)
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

// workingSetAndHead is one branch's contribution to an atomic transaction.
// The working set is always committed, but the commit is optionally nil
type workingSetAndHead struct {
	// dbName is the revision-qualified name of the database that the working set and commit belong to
	dbName string
	// workingSet is the working set to commit, typically from the session
	workingSet *doltdb.WorkingSet
	// commit is the dolt commit to create, or nil if only the working set is being committed
	commit *doltdb.PendingCommit
}

// doCommit commits the working set and creates a new DoltCommit as specified, in one atomic write
func (tx *DoltTransaction) doCommit(
	ctx *sql.Context,
	workingSet *doltdb.WorkingSet,
	commit *doltdb.PendingCommit,
	dbName string,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	workingSets, commits, err := tx.commitHeads(ctx, []workingSetAndHead{{
		dbName:     dbName,
		workingSet: workingSet,
		commit:     commit,
	}})
	if err != nil {
		return nil, nil, err
	}

	// limited to a single element, as above
	return workingSets[0], commits[0], nil
}

// commitHeads atomically commits the head updates given
func (tx *DoltTransaction) commitHeads(
	ctx *sql.Context,
	changes []workingSetAndHead,
) ([]*doltdb.WorkingSet, []*doltdb.Commit, error) {
	if len(changes) == 0 {
		return nil, nil, nil
	}

	sess := DSessFromSess(ctx.Session)
	states := make([]*branchState, len(changes))
	workingSetsAtTxStart := make([]*doltdb.WorkingSet, len(changes))
	lockIDs := make([]string, len(changes))

	var startPoint dbRoot
	var baseDbName string
	for i, change := range changes {
		state, ok, err := sess.lookupDbState(ctx, change.dbName)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, sql.ErrDatabaseNotFound.New(change.dbName)
		}
		normalizedName := strings.ToLower(state.dbState.dbName)

		if i > 0 && normalizedName != baseDbName {
			return nil, nil, ErrMultipleDatabases
		}

		baseDbName = normalizedName
		startPoint, ok = tx.dbStartPoints[baseDbName]
		if !ok {
			return nil, nil, fmt.Errorf("database %s unknown to transaction", change.dbName)
		}
		states[i] = state
		workingSetsAtTxStart[i], err = startPoint.db.ResolveWorkingSetAtRoot(ctx, change.workingSet.Ref(), startPoint.rootHash)
		if err != nil {
			return nil, nil, err
		}

		lockIDs[i] = baseDbName + "\u0000" + change.workingSet.Ref().String()
	}

	// All callers acquire branch locks in the same order.
	// This makes deadlock between two transactions impossible.
	sort.Strings(lockIDs)
	for i, id := range lockIDs {
		if i > 0 && id == lockIDs[i-1] {
			return nil, nil, fmt.Errorf("duplicate working set in transaction: %s", id)
		}
		if err := sess.Provider().TxLocks().Lock(ctx, id); err != nil {
			return nil, nil, err
		}
		defer sess.Provider().TxLocks().Unlock(id)
	}

	name, email, _, _, err := ResolveNameEmail(ctx, DoltCommitterName, DoltCommitterEmail)
	if err != nil {
		return nil, nil, err
	}

	for attempt := 0; attempt < maxTxCommitRetries; attempt++ {
		updates := make([]doltdb.HeadUpdate, 0, 2*len(changes))
		workingSets := make([]*doltdb.WorkingSet, len(changes))
		for i, change := range changes {
			ws := change.workingSet
			existing, err := startPoint.db.ResolveWorkingSet(ctx, ws.Ref())
			newWorkingSet := errors.Is(err, doltdb.ErrWorkingSetNotFound)
			if newWorkingSet {
				existing = doltdb.EmptyWorkingSet(ws.Ref())
			} else if err != nil {
				return nil, nil, err
			}

			existingHash, err := existing.HashOf()
			if err != nil {
				return nil, nil, err
			}

			if err := tx.validateAmendedHead(ctx, startPoint.db, ws, change.commit); err != nil {
				return nil, nil, err
			}

			ff := isFfMerge
			if !newWorkingSet && !workingAndStagedEqual(existing, workingSetsAtTxStart[i]) {
				ff = notFfMerge
				ws, err = tx.mergeRoots(ctx, change.dbName, workingSetsAtTxStart[i], existing, ws, states[i].EditOpts())
				if err != nil {
					return nil, nil, err
				}
			}

			if err := tx.validateWorkingSetForCommit(ctx, ws, ff); err != nil {
				return nil, nil, err
			}

			pending := change.commit
			var expectedHead hash.Hash
			if pending != nil {
				headRef, err := ws.Ref().ToHeadRef()
				if err != nil {
					return nil, nil, err
				}
				head, err := startPoint.db.GetHashForRefStr(ctx, headRef.String())
				if err != nil {
					return nil, nil, err
				}
				expectedHead = *head
				ws, pending, err = prepareDoltCommit(ctx, change.dbName, startPoint.db, workingSetsAtTxStart[i], pending, ws, states[i].EditOpts())
				if err != nil {
					return nil, nil, err
				}
				if err := tx.validateWorkingSetForCommit(ctx, ws, ff); err != nil {
					return nil, nil, err
				}
			}
			workingSets[i] = ws
			updates = append(updates, doltdb.WorkingSetUpdate{
				WorkingSet: ws,
				PrevHash:   existingHash,
				Meta:       tx.WorkingSetMeta(name, email),
			})
			if pending != nil {
				headRef, err := ws.Ref().ToHeadRef()
				if err != nil {
					return nil, nil, err
				}
				updates = append(updates, doltdb.BranchHeadUpdate{
					HeadRef:       headRef,
					Root:          pending.Roots.Staged,
					CommitOptions: pending.CommitOptions,
					ExpectedHead:  expectedHead,
					WorkingSetRef: ws.Ref(),
					PrevWsHash:    existingHash,
				})
			}
		}

		var rsc doltdb.ReplicationStatusController
		heads, err := startPoint.db.CommitHeadUpdates(ctx, updates, &rsc)
		WaitForReplicationController(ctx, rsc)

		// The check in doCommit can go stale before the ref update, so the storage layer compares the head against
		// AmendedCommit once more, atomically with the update. A failure there surfaces the same way.
		if errors.Is(err, datas.ErrMergeNeeded) {
			for _, change := range changes {
				if change.commit != nil && !change.commit.CommitOptions.AmendedCommit.IsEmpty() {
					return nil, nil, tx.rollbackAndErr(ctx, retryTransactionError(err.Error()))
				}
			}
		}

		// An optimistic lock failure will be retried with the new current value of the working set.
		if errors.Is(err, datas.ErrOptimisticLockFailed) || errors.Is(err, datas.ErrMergeNeeded) {
			continue
		}

		if err != nil {
			return nil, nil, err
		}
		commits := make([]*doltdb.Commit, len(changes))
		index := 0
		for i, change := range changes {
			index++ // Each working set is followed by its optional branch head.
			if change.commit != nil {
				commits[i], err = doltdb.HashToCommit(ctx, startPoint.db.ValueReadWriter(), startPoint.db.NodeStore(), heads[index])
				if err != nil {
					return nil, nil, err
				}
				index++
			}
		}
		for _, state := range states {
			doltdb.BranchActivityWriteEvent(ctx, state.dbState.dbName, state.head)
		}
		return workingSets, commits, nil
	}

	return nil, nil, datas.ErrOptimisticLockFailed
}

// mergeRoots merges the roots in the existing working set with the one being committed and returns the resulting
// working set. Conflicts are automatically resolved with "accept ours" if the session settings dictate it.
// Currently merges working and staged roots as necessary. HEAD root is only handled by the DoltCommit function.
func (tx *DoltTransaction) mergeRoots(
	ctx *sql.Context,
	dbName string,
	startState *doltdb.WorkingSet,
	existingWorkingSet *doltdb.WorkingSet,
	workingSet *doltdb.WorkingSet,
	mergeOpts editor.Options,
) (*doltdb.WorkingSet, error) {
	tableResolver, err := GetTableResolver(ctx, dbName)
	if err != nil {
		return nil, err
	}

	if !rootsEqual(existingWorkingSet.WorkingRoot(), workingSet.WorkingRoot()) {
		result, err := merge.MergeRoots(ctx, tableResolver, existingWorkingSet.WorkingRoot(), workingSet.WorkingRoot(), startState.WorkingRoot(), workingSet, startState, mergeOpts, merge.MergeOpts{})
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithWorkingRoot(result.Root)
	}

	if !rootsEqual(existingWorkingSet.StagedRoot(), workingSet.StagedRoot()) {
		result, err := merge.MergeRoots(ctx, tableResolver, existingWorkingSet.StagedRoot(), workingSet.StagedRoot(), startState.StagedRoot(), workingSet, startState, mergeOpts, merge.MergeOpts{})
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

// rollbackAndErr rolls the transaction back and returns |err|, or the rollback error when rolling back fails.
func (tx *DoltTransaction) rollbackAndErr(ctx *sql.Context, err error) error {
	if rollbackErr := tx.rollback(ctx); rollbackErr != nil {
		return rollbackErr
	}
	return err
}

// retryTransactionError returns the client facing serialization failure for a transaction that conflicts with a
// committed transaction. A non empty |detail| is placed before the retry advice.
func retryTransactionError(detail string) error {
	if detail == "" {
		return sql.ErrLockDeadlock.New(ErrRetryTransaction.Error())
	}
	return sql.ErrLockDeadlock.New(fmt.Sprintf("%s: %s", detail, ErrRetryTransaction.Error()))
}

// validateAmendedHead returns a retryable error when |pending| amends a commit that is no longer the head of the
// branch that |workingSet| belongs to. The transaction is rolled back before the error is returned. A nil
// |pending| or an ordinary commit passes without any check.
func (tx *DoltTransaction) validateAmendedHead(ctx *sql.Context, doltDb *doltdb.DoltDB, workingSet *doltdb.WorkingSet, pending *doltdb.PendingCommit) error {
	if pending == nil {
		return nil
	}
	amended := pending.CommitOptions.AmendedCommit
	if amended.IsEmpty() {
		return nil
	}

	headRef, err := workingSet.Ref().ToHeadRef()
	if err != nil {
		return err
	}
	curHeadAddr, err := doltDb.GetHashForRefStr(ctx, headRef.String())
	if err != nil {
		return err
	}
	if *curHeadAddr != amended {
		return tx.rollbackAndErr(ctx, retryTransactionError(fmt.Sprintf(
			"cannot amend head of branch '%s': is at %s but expected %s",
			headRef.GetPath(), *curHeadAddr, amended)))
	}
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
			return tx.rollbackAndErr(ctx, retryTransactionError(""))
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
			tablesWithViolations, err := doltdb.TablesWithConstraintViolations(ctx, workingRoot)
			if err != nil {
				return err
			}

			var messageBuilder strings.Builder
			for tableIndex, tableName := range tablesWithViolations {
				table, _, err := workingRoot.GetTable(ctx, tableName)
				if err != nil {
					return err
				}

				artifactIndex, err := table.GetArtifacts(ctx)
				if err != nil {
					return err
				}

				artifactMap := durable.ProllyMapFromArtifactIndex(artifactIndex)
				cvIterator, err := artifactMap.IterAllCVs(ctx)
				if err != nil {
					return err
				}

				countByDescription := make(map[string]int, 8)
				for {
					artifact, err := cvIterator.Next(ctx)
					if err != nil {
						break
					}

					var constraintViolationMeta prolly.ConstraintViolationMeta
					err = json.Unmarshal(artifact.Metadata, &constraintViolationMeta)
					if err != nil {
						return err
					}

					var description string
					switch artifact.ArtType {
					case prolly.ArtifactTypeForeignKeyViol:
						var foreignKeyMeta merge.FkCVMeta
						err = json.Unmarshal(constraintViolationMeta.VInfo, &foreignKeyMeta)
						if err != nil {
							return err
						}
						description = fmt.Sprintf("\n"+
							"Type: Foreign Key Constraint Violation\n"+
							"\tForeignKey: %s,\n"+
							"\tTable: %s,\n"+
							"\tReferencedTable: %s,\n"+
							"\tIndex: %s,\n"+
							"\tReferencedIndex: %s", foreignKeyMeta.ForeignKey, foreignKeyMeta.Table, foreignKeyMeta.ReferencedTable, foreignKeyMeta.Index, foreignKeyMeta.ReferencedIndex)

					case prolly.ArtifactTypeUniqueKeyViol:
						var uniqueKeyMeta merge.UniqCVMeta
						err = json.Unmarshal(constraintViolationMeta.VInfo, &uniqueKeyMeta)
						if err != nil {
							return err
						}
						description = fmt.Sprintf("\n"+
							"Type: Unique Key Constraint Violation,\n"+
							"\tName: %s,\n"+
							"\tColumns: %v", uniqueKeyMeta.Name, uniqueKeyMeta.Columns)

					case prolly.ArtifactTypeNullViol:
						var nullViolationMeta merge.NullViolationMeta
						err = json.Unmarshal(constraintViolationMeta.VInfo, &nullViolationMeta)
						if err != nil {
							return err
						}
						description = fmt.Sprintf("\n"+
							"Type: Null Constraint Violation,\n"+
							"\tColumns: %v", nullViolationMeta.Columns)

					case prolly.ArtifactTypeChkConsViol:
						var checkConstraintMeta merge.CheckCVMeta
						err = json.Unmarshal(constraintViolationMeta.VInfo, &checkConstraintMeta)
						if err != nil {
							return err
						}
						description = fmt.Sprintf("\n"+
							"Type: Check Constraint Violation,\n"+
							"\tName: %s,\n"+
							"\tExpression: %v", checkConstraintMeta.Name, checkConstraintMeta.Expression)
					}
					if err != nil {
						return err
					}
					countByDescription[description]++
				}

				sortedDescriptions := make([]string, 0, len(countByDescription))
				for descriptionText := range countByDescription {
					sortedDescriptions = append(sortedDescriptions, descriptionText)
				}
				sort.Strings(sortedDescriptions)
				if tableIndex > 0 {
					messageBuilder.WriteString(", ")
				}
				for _, description := range sortedDescriptions {
					messageBuilder.WriteString(description)
					if rowCount := countByDescription[description]; rowCount > 1 {
						messageBuilder.WriteString(fmt.Sprintf(" (%d row(s))", rowCount))
					}
				}
			}

			return tx.rollbackAndErr(ctx, fmt.Errorf("%s%s%s", ErrUnresolvedConstraintViolationsCommit, ConstraintViolationsListPrefix, messageBuilder.String()))
		}
	}

	return nil
}

// CreateSavepoint creates a new savepoint with the name and roots given. If a savepoint with the name given
// already exists, it's overwritten.
func (tx *DoltTransaction) CreateSavepoint(name string, roots map[string]doltdb.RootValue, dirtyBranches map[string]bool) {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		tx.savepoints = append(tx.savepoints[:existing], tx.savepoints[existing+1:]...)
	}
	tx.savepoints = append(tx.savepoints, savepoint{name: name, roots: roots, dirtyBranches: dirtyBranches})
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

// RollbackToSavepoint returns the saved branch roots and dirty flags, or nil if no such savepoint can
// be found. All savepoints created after the one being rolled back to are no longer accessible.
func (tx *DoltTransaction) RollbackToSavepoint(name string) *savepoint {
	existing := tx.findSavepoint(name)
	if existing >= 0 {
		// Clear out any savepoints past this one
		tx.savepoints = tx.savepoints[:existing+1]
		return &tx.savepoints[existing]
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

// WorkingSetMeta returns the metadata to use for a commit of this transaction.
func (tx *DoltTransaction) WorkingSetMeta(name, email string) *datas.WorkingSetMeta {
	return &datas.WorkingSetMeta{
		Name:        name,
		Email:       email,
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
