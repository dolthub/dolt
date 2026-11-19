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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
)

// rowLockRead is the state of a table that locks the rows it returns, as the tables of a SELECT with a locking
// clause do. It is shared by every copy of the table made after the clause was applied.
type rowLockRead struct {
	spec sql.RowLockSpec

	mu sync.Mutex
	// keys makes row journal keys for the table's rows. It is built when first needed and rebuilt if the table's
	// schema changes, which can happen between executions of a cached plan.
	keys *writer.RowLockKeys
}

// RowLockingTable is implemented by the tables of this package that can lock the rows they return.
type RowLockingTable interface {
	// LocksRows returns whether the table locks the rows it returns.
	LocksRows() bool
}

var _ sql.RowLockingTable = (*DoltTable)(nil)
var _ sql.RowLockingTable = (*WritableDoltTable)(nil)
var _ RowLockingTable = (*DoltTable)(nil)

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

// LocksRows implements RowLockingTable.
func (t *DoltTable) LocksRows() bool {
	return t.rowLock != nil
}

// readCols returns the tags of the columns to read from storage for this table's rows. A table that locks the rows
// it returns reads every column, because the key that identifies a row for locking is made from the whole row.
func (t *DoltTable) readCols() []uint64 {
	if t.rowLock != nil {
		return t.sch.GetAllCols().Tags
	}

	return t.projectedCols
}

// withRowLocks returns |iter| unchanged for a table that doesn't lock the rows it returns. For one that does, |iter|
// must return rows with the columns of readCols, and the iterator returned locks each of those rows in the row
// journal before returning it with only the table's projected columns.
func (t *DoltTable) withRowLocks(ctx *sql.Context, iter sql.ValueRowWriter) (sql.ValueRowWriter, error) {
	if t.rowLock == nil {
		return iter, nil
	}

	locking, err := t.newRowLockingIter(ctx, iter)
	if err != nil {
		_ = iter.Close(ctx)
		return nil, err
	}

	return locking, nil
}

func (t *DoltTable) newRowLockingIter(ctx *sql.Context, iter sql.RowIter) (*rowLockingIter, error) {
	locking := &rowLockingIter{
		child:      iter,
		projection: t.rowLockProjection(),
		table:      t.tableName,
	}

	locks, ok := dsess.RowLocksForContext(ctx)
	if !ok || t.lockedToRoot != nil || t.overriddenSchema != nil {
		// Rows are only locked for a transaction that takes row locks, and only where they can be changed: in the
		// working set of a branch.
		return locking, nil
	}

	sess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := sess.WorkingSet(ctx, t.db.RevisionQualifiedName())
	if err != nil || workingSet == nil {
		return locking, nil
	}

	keys, err := t.rowLockKeys(ctx)
	if err != nil {
		return nil, err
	}

	locking.locks = locks
	locking.keys = keys
	locking.request = rowjournal.LockRequest{
		Relation:   dsess.RowLockRelation(t.db.AliasedName(), workingSet.Ref(), t.TableName()),
		Compare:    keys.Compare,
		KeyFormat:  keys.KeyFormat,
		Mode:       rowLockMode(t.rowLock.spec.Strength),
		WaitPolicy: rowLockWaitPolicy(t.rowLock.spec.WaitPolicy),
	}
	return locking, nil
}

// rowLockKeys returns the maker of row journal keys for this table's rows as they are now.
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

// rowLockProjection returns, for each of this table's projected columns, its position among the columns of
// readCols. It returns nil when the two are the same.
func (t *DoltTable) rowLockProjection() []int {
	if t.projectedCols == nil {
		return nil
	}

	positions := make(map[uint64]int)
	for i, tag := range t.sch.GetAllCols().Tags {
		positions[tag] = i
	}

	projection := make([]int, len(t.projectedCols))
	for i, tag := range t.projectedCols {
		projection[i] = positions[tag]
	}

	return projection
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

// rowLockingIter returns the rows of a child iterator after locking each of them in the row journal. A row that
// can't be locked is waited for, left out, or reported as an error, as the lock request's wait policy says. A row
// that a transaction this one can't see has changed or deleted is reported as an error, since this transaction's
// view of it is out of date.
type rowLockingIter struct {
	child sql.RowIter
	// locks and keys are unset when the rows need no locking.
	locks   dsess.RowLocks
	keys    *writer.RowLockKeys
	request rowjournal.LockRequest
	// projection is the positions in the child's rows of the columns to return, or nil to return them all.
	projection []int
	// table is the name of the table the rows are read from.
	table string
}

var _ sql.RowIter = (*rowLockingIter)(nil)
var _ sql.ValueRowWriter = (*rowLockingIter)(nil)

// Next implements sql.RowIter.
func (i *rowLockingIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			return nil, err
		}

		locked, err := i.lock(ctx, row)
		if err != nil {
			return nil, err
		}

		if !locked {
			continue
		}

		if i.projection == nil {
			return row, nil
		}

		projected := make(sql.Row, len(i.projection))
		for to, from := range i.projection {
			projected[to] = row[from]
		}

		return projected, nil
	}
}

// lock locks |row| and returns whether it was locked. A row that wasn't is to be left out of the result.
func (i *rowLockingIter) lock(ctx *sql.Context, row sql.Row) (bool, error) {
	if i.keys == nil {
		return true, nil
	}

	key, err := i.keys.Key(ctx, row)
	if err != nil {
		return false, err
	}

	req := i.request
	req.Target = rowjournal.RowTarget(key)
	req.Timeout = dsess.RowLockWaitTimeout(ctx)

	res, err := i.locks.Journal.Lock(ctx, i.locks.Txn, req)
	if err != nil {
		return false, dsess.RowLockWaitError(err, i.table)
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

// Close implements sql.RowIter.
func (i *rowLockingIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

// WriteNextValueRow implements sql.ValueRowWriter. It is never called, because IsValueRowWriter returns false.
func (i *rowLockingIter) WriteNextValueRow(*sql.Context, sql.ValueRow) error {
	panic("rowLockingIter does not write value rows")
}

// IsValueRowWriter implements sql.ValueRowWriter. Rows are locked by keys made from sql.Rows, so that is the only
// form this iterator returns them in.
func (i *rowLockingIter) IsValueRowWriter(*sql.Context) bool {
	return false
}

// Columns implements sql.ValueRowWriter.
func (i *rowLockingIter) Columns() sql.ColSet {
	return sql.ColSet{}
}
