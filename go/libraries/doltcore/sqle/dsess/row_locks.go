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
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
	"github.com/dolthub/dolt/go/store/hash"
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

// lockRewrittenTables takes, for the transaction of |ctx|, a lock on each table as a whole whose definition or
// contents differ between the working root of |bs| and |newRoot|, which is about to replace it. Everything that
// replaces a working root this way changes tables without going through their writers, and so without locking the rows
// it changes: DDL statements, and version control operations such as merge, reset and checkout. Locking the table as a
// whole makes such an operation wait for the transactions that hold locks on the table's rows, and makes later
// statements that touch the table wait for it. The change is also recorded against the table, so that a transaction
// whose snapshot predates it can't lock or change the table's rows afterwards.
func (d *DoltSession) lockRewrittenTables(ctx *sql.Context, bs *branchState, newRoot doltdb.RootValue) error {
	locks, ok := RowLocksForContext(ctx)
	if !ok || d.restoringSavepoint {
		return nil
	}

	workingSet, ok := bs.workingSetRef()
	if !ok {
		return nil
	}

	oldRoot := bs.workingSet.WorkingRoot()
	if rootsEqual(oldRoot, newRoot) {
		return nil
	}

	changes, err := tableChanges(ctx, oldRoot, newRoot)
	if err != nil {
		return err
	}

	for _, change := range changes {
		relation := RowLockRelation(bs.dbState.dbName, workingSet, change.name)
		res, err := locks.Journal.Write(ctx, locks.Txn, rowjournal.WriteRequest{
			Relation: relation,
			Target:   rowjournal.TableTarget(),
			Op:       rowjournal.WriteUpdate,
			// The change may have deleted rows or changed their keys, which calls for the strongest lock.
			KeysChanged: true,
			Timeout:     RowLockWaitTimeout(ctx),
		})
		if err != nil {
			return RowLockWaitError(err, change.name.Name)
		}

		if err = RowLockConflictError(res); err != nil {
			return err
		}

		if change.schemaChanged {
			locks.Journal.RecordSchemaChange(locks.Txn, rowjournal.SchemaChangeRecord{
				Relation: relation,
				Kind:     change.kind,
			})
		}
	}

	return nil
}

// tableChange describes how a table differs between two roots.
type tableChange struct {
	name doltdb.TableName
	// kind is whether the table was created, dropped, or changed.
	kind rowjournal.SchemaChangeKind
	// schemaChanged is whether the table's definition differs, and not only its rows.
	schemaChanged bool
}

// tableChanges returns the tables that differ between |oldRoot| and |newRoot|, in order of name.
func tableChanges(ctx *sql.Context, oldRoot, newRoot doltdb.RootValue) ([]tableChange, error) {
	oldNames, err := oldRoot.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}

	newNames, err := newRoot.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}

	var changes []tableChange
	inOld := make(map[doltdb.TableName]struct{}, len(oldNames))
	for _, name := range oldNames {
		inOld[name] = struct{}{}
		oldHash, _, err := oldRoot.GetTableHash(ctx, name)
		if err != nil {
			return nil, err
		}

		newHash, ok, err := newRoot.GetTableHash(ctx, name)
		if err != nil {
			return nil, err
		}

		if !ok {
			changes = append(changes, tableChange{name: name, kind: rowjournal.SchemaDropTable, schemaChanged: true})
			continue
		}

		if oldHash == newHash {
			continue
		}

		oldSchema, err := oldRoot.GetTableSchemaHash(ctx, name)
		if err != nil {
			return nil, err
		}

		newSchema, err := newRoot.GetTableSchemaHash(ctx, name)
		if err != nil {
			return nil, err
		}

		changes = append(changes, tableChange{name: name, kind: rowjournal.SchemaAlterTable, schemaChanged: oldSchema != newSchema})
	}

	for _, name := range newNames {
		if _, ok := inOld[name]; !ok {
			changes = append(changes, tableChange{name: name, kind: rowjournal.SchemaCreateTable, schemaChanged: true})
		}
	}

	sort.Slice(changes, func(i, j int) bool { return changes[i].name.Less(changes[j].name) })
	return changes, nil
}

// RecordRefUpdate appends to the engine's row journal a record of a change that a version control operation made to
// a ref of the database named. |newRef| is the ref's new name for a rename, and nil otherwise. Deleting or renaming
// a branch does the same to the row locks held on the branch's tables.
func RecordRefUpdate(ctx *sql.Context, database string, kind rowjournal.RefUpdateKind, updated, newRef ref.DoltRef, oldHash, newHash hash.Hash) {
	sess, ok := ctx.Session.(*DoltSession)
	if !ok {
		return
	}

	journal := sess.rowJournal()
	if journal == nil {
		return
	}

	rec := rowjournal.RefUpdateRecord{
		Database: strings.ToLower(database),
		Kind:     kind,
		Ref:      updated.String(),
		Branch:   rowLockBranch(updated),
		OldHash:  oldHash,
		NewHash:  newHash,
	}
	if newRef != nil {
		rec.NewRef = newRef.String()
		rec.NewBranch = rowLockBranch(newRef)
	}

	var txn *rowjournal.Txn
	if tx, ok := ctx.GetTransaction().(*DoltTransaction); ok {
		txn = tx.rowTxn
	}

	journal.RecordRefUpdate(txn, rec)
}

// rowLockBranch returns the row journal's name for the tables on a ref, or the empty string for a ref that has no
// working set, and so no tables that can be locked.
func rowLockBranch(r ref.DoltRef) string {
	workingSet, err := ref.WorkingSetRefForHead(r)
	if err != nil {
		return ""
	}

	return workingSet.GetPath()
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

// RowLockWaitError translates an error returned by the row journal for an abandoned request for a lock in the table
// named into the SQL error to report. Other errors, such as a canceled context's, are returned as they are.
func RowLockWaitError(err error, table string) error {
	switch {
	case errors.Is(err, rowjournal.ErrDeadlock):
		return sql.ErrRowLockDeadlock.New()
	case errors.Is(err, rowjournal.ErrLockTimeout):
		return sql.ErrRowLockWaitTimeout.New()
	case errors.Is(err, rowjournal.ErrLockNotAvailable):
		return sql.ErrRowLockNotAvailable.New(table)
	case errors.Is(err, rowjournal.ErrRelationChanged):
		return sql.ErrLockDeadlock.New(err.Error())
	default:
		return err
	}
}
