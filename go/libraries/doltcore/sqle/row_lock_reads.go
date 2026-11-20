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

package sqle

import (
	"context"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/store/val"
)

// rowLockRead is the state of a table that locks the rows it returns, as the tables of a SELECT with a locking
// clause do. It is shared by every copy of the table made after the clause was applied.
type rowLockRead struct {
	spec sql.RowLockSpec

	mu sync.Mutex
	// keys describes the row journal keys of the table's rows. It is built when first needed and rebuilt if the
	// table's schema changes, which can happen between executions of a cached plan.
	keys *writer.RowLockKeys
}

var _ sql.RowLockingTable = (*DoltTable)(nil)
var _ sql.RowLockingTable = (*WritableDoltTable)(nil)

// WithRowLock implements sql.RowLockingTable.
func (t *DoltTable) WithRowLock(spec sql.RowLockSpec) sql.Table {
	nt := *t
	nt.rowLock = &rowLockRead{spec: spec}
	return &nt
}

// WithRowLock implements sql.RowLockingTable.
func (t *WritableDoltTable) WithRowLock(spec sql.RowLockSpec) sql.Table {
	nt := *t
	nt.DoltTable = t.DoltTable.WithRowLock(spec).(*DoltTable)
	return &nt
}

// RowLocker returns the means to lock the rows of this table as the statement of |ctx| reads them, or nil if they
// are to be read without locking: because the table has no locking clause, because the transaction takes no row
// locks, or because the rows aren't ones that can be changed. Every reader of the table's rows, at any level, must
// lock the rows it returns with it.
func (t *DoltTable) RowLocker(ctx *sql.Context) (index.RowLocker, error) {
	if t.rowLock == nil {
		return nil, nil
	}

	locks, ok := dsess.RowLocksForContext(ctx)
	if !ok || t.lockedToRoot != nil || t.overriddenSchema != nil {
		// Rows are only locked for a transaction that takes row locks, and only where they can be changed: in the
		// working set of a branch.
		return nil, nil
	}

	sess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := sess.WorkingSet(ctx, t.db.RevisionQualifiedName())
	if err != nil || workingSet == nil {
		return nil, nil
	}

	keys, err := t.rowLockKeys(ctx)
	if err != nil {
		return nil, err
	}

	return &tableRowLocker{
		locks: locks,
		request: rowjournal.LockRequest{
			Relation:   dsess.RowLockRelation(t.db.AliasedName(), workingSet.Ref(), t.TableName()),
			Compare:    keys.Compare,
			KeyFormat:  keys.KeyFormat,
			Mode:       rowLockMode(t.rowLock.spec.Strength),
			WaitPolicy: rowLockWaitPolicy(t.rowLock.spec.WaitPolicy),
			Timeout:    dsess.RowLockWaitTimeout(ctx),
		},
		table: t.tableName,
	}, nil
}

// lockingBuilder returns |lb| made to lock the rows it reads, if this table locks the rows it returns.
func (t *DoltTable) lockingBuilder(ctx *sql.Context, lb index.IndexScanBuilder) (index.IndexScanBuilder, error) {
	locker, err := t.RowLocker(ctx)
	if err != nil {
		return nil, err
	}

	return lb.WithRowLocker(locker), nil
}

// rowLockKeys returns the description of the row journal keys of this table's rows as they are now.
func (t *DoltTable) rowLockKeys(ctx *sql.Context) (*writer.RowLockKeys, error) {
	table, err := t.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	keyFormat, err := table.GetSchemaHash(ctx)
	if err != nil {
		return nil, err
	}

	t.rowLock.mu.Lock()
	defer t.rowLock.mu.Unlock()
	if t.rowLock.keys == nil || t.rowLock.keys.KeyFormat != keyFormat {
		t.rowLock.keys, err = writer.NewRowLockKeys(ctx, table, t.TableName(), t.db.RevisionQualifiedName())
		if err != nil {
			return nil, err
		}
	}

	return t.rowLock.keys, nil
}

func rowLockMode(strength sql.RowLockStrength) rowjournal.LockMode {
	switch strength {
	case sql.RowLockKeyShare:
		return rowjournal.LockKeyShare
	case sql.RowLockShare:
		return rowjournal.LockShare
	case sql.RowLockNoKeyUpdate:
		return rowjournal.LockNoKeyUpdate
	default:
		return rowjournal.LockUpdate
	}
}

func rowLockWaitPolicy(policy sql.RowLockWaitPolicy) rowjournal.WaitPolicy {
	switch policy {
	case sql.RowLockSkipLocked:
		return rowjournal.WaitSkip
	case sql.RowLockNoWait:
		return rowjournal.WaitError
	default:
		return rowjournal.WaitBlock
	}
}

// tableRowLocker locks rows of one table in the row journal, for one transaction. A row that can't be locked is
// waited for, left out, or reported as an error, as the lock request's wait policy says. A row that a transaction
// this one can't see has changed or deleted is reported as an error, since this transaction's view of it is out of
// date.
type tableRowLocker struct {
	locks dsess.RowLocks
	// request is the lock request for a row, complete but for the row.
	request rowjournal.LockRequest
	// table is the name of the table the rows are read from.
	table string
}

var _ index.RowLocker = (*tableRowLocker)(nil)

// LockRow implements index.RowLocker.
func (l *tableRowLocker) LockRow(ctx context.Context, key val.Tuple) (bool, error) {
	req := l.request
	req.Target = rowjournal.RowTarget(key)

	res, err := l.locks.Journal.Lock(ctx, l.locks.Txn, req)
	if err != nil {
		return false, dsess.RowLockWaitError(err, l.table)
	}

	switch res {
	case rowjournal.LockGranted:
		return true, nil
	case rowjournal.LockWouldBlock:
		return false, nil
	default:
		return false, dsess.RowLockConflictError(res)
	}
}
