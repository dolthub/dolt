// Copyright 2026 Dolthub, Inc.
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
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
)

// RowLocks takes row locks for one transaction in the engine's row journal.
type RowLocks struct {
	Journal *rowjournal.Journal
	Txn     *rowjournal.Txn
}

// RowLocksForContext returns the means to take row locks for the transaction of |ctx|, or false if that transaction
// takes none: because row-level locking is turned off, or because it isn't a transaction the row journal knows.
func RowLocksForContext(ctx *sql.Context) (RowLocks, bool) {
	tx, ok := ctx.GetTransaction().(*DoltTransaction)
	if !ok || !tx.rowLocking || tx.rowTxn == nil {
		return RowLocks{}, false
	}

	return RowLocks{Journal: tx.rowJournal, Txn: tx.rowTxn}, true
}

// RowLockingOwner is implemented by a WorkingSetOwner whose rows other transactions can see, and so must be locked
// before its write session changes them.
type RowLockingOwner interface {
	// RowLockBranch returns the database and the working set of the branch that the owner's write session changes,
	// or false if its changes go somewhere no other transaction can see.
	RowLockBranch() (database string, workingSet ref.WorkingSetRef, ok bool)
}

var _ RowLockingOwner = branchWorkingSetOwner{}

// RowLockBranch implements RowLockingOwner.
func (o branchWorkingSetOwner) RowLockBranch() (string, ref.WorkingSetRef, bool) {
	wsRef, ok := o.bs.workingSetRef()
	if !ok {
		return "", ref.WorkingSetRef{}, false
	}

	return o.bs.dbState.dbName, wsRef, true
}

// RowLockRelation returns the row journal's name for a table on a branch.
func RowLockRelation(database string, workingSet ref.WorkingSetRef, table doltdb.TableName) rowjournal.RelationKey {
	return rowjournal.RelationKey{
		Database: strings.ToLower(database),
		Branch:   workingSet.GetPath(),
		Table:    table.ToLower(),
	}
}

// RowLockWaitTimeout returns how long a statement run with |ctx| waits for a row lock before giving up, or zero to
// wait indefinitely. It is a variable so that a SQL dialect with its own setting for this can substitute it.
var RowLockWaitTimeout = func(ctx *sql.Context) time.Duration {
	val, err := ctx.GetSessionVariable(ctx, "innodb_lock_wait_timeout")
	if err != nil {
		return 0
	}

	seconds, ok := val.(int64)
	if !ok || seconds <= 0 {
		return 0
	}

	return time.Duration(seconds) * time.Second
}

// RowLockConflictError returns the error for a row lock or row write that was refused because a transaction that the
// requester cannot see committed a change to the row. It returns nil for a result that isn't a refusal.
func RowLockConflictError(res rowjournal.LockResult) error {
	switch res {
	case rowjournal.LockRowUpdated:
		return sql.ErrLockDeadlock.New("could not serialize access due to concurrent update")
	case rowjournal.LockRowDeleted:
		return sql.ErrLockDeadlock.New("could not serialize access due to concurrent delete")
	case rowjournal.LockKeyExists:
		return sql.ErrLockDeadlock.New("could not serialize access due to concurrent insert of the same key")
	default:
		return nil
	}
}

// RowLockWaitError translates an error returned by the row journal for an abandoned lock request into the SQL error
// to report. Other errors, such as a canceled context's, are returned as they are.
func RowLockWaitError(err error) error {
	switch {
	case errors.Is(err, rowjournal.ErrDeadlock):
		return sql.ErrRowLockDeadlock.New()
	case errors.Is(err, rowjournal.ErrLockTimeout):
		return sql.ErrRowLockWaitTimeout.New()
	case errors.Is(err, rowjournal.ErrLockNotAvailable):
		return sql.ErrRowLockNotAvailable.New()
	case errors.Is(err, rowjournal.ErrRelationChanged):
		return sql.ErrLockDeadlock.New(err.Error())
	default:
		return err
	}
}
