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

package writer

import (
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// rowLockKeyer is implemented by the index writers of a table's primary index, to say how the table's rows are
// identified for row locking.
type rowLockKeyer interface {
	// rowLockKey returns the key that identifies |sqlRow| in the row journal.
	rowLockKey(ctx context.Context, sqlRow sql.Row) (val.Tuple, error)
	// rowLockCompare returns the ordering of the keys that rowLockKey returns.
	rowLockCompare() rowjournal.KeyCompare
	// rowLockKeysAreContents returns whether a row's key is derived from all of its columns, as it is in a table
	// without a primary key. Such a key identifies every row with the same contents, and changes with any update.
	rowLockKeysAreContents() bool
}

var _ rowLockKeyer = prollyIndexWriter{}
var _ rowLockKeyer = prollyKeylessWriter{}

func (m prollyIndexWriter) rowLockKey(ctx context.Context, sqlRow sql.Row) (val.Tuple, error) {
	return m.keyFromRow(ctx, sqlRow)
}

func (m prollyIndexWriter) rowLockCompare() rowjournal.KeyCompare {
	desc := m.keyBld.Desc
	return func(left, right []byte) int {
		cmp, err := desc.Compare(context.Background(), val.Tuple(left), val.Tuple(right))
		if err != nil {
			// Comparing can only fail for a key field stored out of line that can't be read. Ordering by bytes
			// keeps such keys distinct from one another, which is the safe way to be wrong for locking.
			return bytes.Compare(left, right)
		}

		return cmp
	}
}

func (m prollyIndexWriter) rowLockKeysAreContents() bool {
	return false
}

func (k prollyKeylessWriter) rowLockKey(ctx context.Context, sqlRow sql.Row) (val.Tuple, error) {
	hashId, _, err := k.tuplesFromRow(ctx, sqlRow)
	return hashId, err
}

func (k prollyKeylessWriter) rowLockCompare() rowjournal.KeyCompare {
	return bytes.Compare
}

func (k prollyKeylessWriter) rowLockKeysAreContents() bool {
	return true
}

// rowLockTarget is what a table writer needs to know to lock the rows of its table. It is worked out when first
// needed and again whenever the write session's working set changes branch.
type rowLockTarget struct {
	workingSet ref.WorkingSetRef
	relation   rowjournal.RelationKey
	keyer      rowLockKeyer
	compare    rowjournal.KeyCompare
	// keyFormat identifies the format of the keys that |keyer| makes, which changes with the table's schema.
	keyFormat hash.Hash
	valid     bool
}

// rowLocks returns the means to lock rows of this writer's table for the transaction of |ctx|, or false if the
// writer's changes need no row locks: because the transaction takes none, or because the changes go somewhere that
// no other transaction can see, like a temporary table.
func (w *prollyTableWriter) rowLocks(ctx *sql.Context) (dsess.RowLocks, bool) {
	locks, ok := dsess.RowLocksForContext(ctx)
	if !ok || w.writeSess.targetStaging {
		return dsess.RowLocks{}, false
	}

	owner, ok := w.writeSess.owner.(dsess.RowLockingOwner)
	if !ok {
		return dsess.RowLocks{}, false
	}

	database, workingSet, ok := owner.RowLockBranch()
	if !ok {
		return dsess.RowLocks{}, false
	}

	if !w.lockTarget.valid || w.lockTarget.workingSet != workingSet {
		keyer, ok := w.primary.(rowLockKeyer)
		if !ok {
			return dsess.RowLocks{}, false
		}

		keyFormat, err := w.tbl.GetSchemaHash(ctx)
		if err != nil {
			return dsess.RowLocks{}, false
		}

		w.lockTarget = rowLockTarget{
			workingSet: workingSet,
			relation:   dsess.RowLockRelation(database, workingSet, w.tblName),
			keyer:      keyer,
			compare:    keyer.rowLockCompare(),
			keyFormat:  keyFormat,
			valid:      true,
		}
	}

	return locks, true
}

// lockRowForInsert takes the row lock that inserting |sqlRow| requires and records the insert in the row journal.
func (w *prollyTableWriter) lockRowForInsert(ctx *sql.Context, sqlRow sql.Row) error {
	locks, ok := w.rowLocks(ctx)
	if !ok {
		return nil
	}

	key, err := w.lockTarget.keyer.rowLockKey(ctx, sqlRow)
	if err != nil {
		return err
	}

	if w.lockTarget.keyer.rowLockKeysAreContents() {
		return w.writeLockedRow(ctx, locks, contentsCountChange(key))
	}

	return w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
		Target: rowjournal.RowTarget(key),
		Op:     rowjournal.WriteInsert,
	})
}

// lockRowForUpdate takes the row locks that updating |oldRow| to |newRow| requires and records the update in the row
// journal.
func (w *prollyTableWriter) lockRowForUpdate(ctx *sql.Context, oldRow, newRow sql.Row) error {
	locks, ok := w.rowLocks(ctx)
	if !ok {
		return nil
	}

	keyer := w.lockTarget.keyer
	oldKey, err := keyer.rowLockKey(ctx, oldRow)
	if err != nil {
		return err
	}

	newKey, err := keyer.rowLockKey(ctx, newRow)
	if err != nil {
		return err
	}

	if keyer.rowLockKeysAreContents() {
		// One fewer row has the old contents and one more has the new.
		err = w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
			Target:      rowjournal.RowTarget(oldKey),
			Op:          rowjournal.WriteUpdate,
			KeysChanged: true,
			Successor:   newKey,
		})
		if err != nil {
			return err
		}

		return w.writeLockedRow(ctx, locks, contentsCountChange(newKey))
	}

	if oldKey.Count() == 0 {
		// There is no old row, which happens when a conflict is resolved for a row that one side didn't have.
		return w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
			Target: rowjournal.RowTarget(newKey),
			Op:     rowjournal.WriteInsert,
		})
	}

	if w.lockTarget.compare(oldKey, newKey) == 0 {
		return w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
			Target: rowjournal.RowTarget(oldKey),
			Op:     rowjournal.WriteUpdate,
		})
	}

	err = w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
		Target:      rowjournal.RowTarget(oldKey),
		Op:          rowjournal.WriteUpdate,
		KeysChanged: true,
		Successor:   newKey,
	})
	if err != nil {
		return err
	}

	return w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
		Target: rowjournal.RowTarget(newKey),
		Op:     rowjournal.WriteInsert,
	})
}

// contentsCountChange returns the write to record when a row is added to a table whose rows are keyed by their
// contents. All the rows with the same contents are stored as one entry with a count, so adding one is a change to
// that entry. Two transactions that each add a row with the same contents must not both build on the same count,
// which is what recording the addition as an update of the entry prevents.
func contentsCountChange(key val.Tuple) rowjournal.WriteRequest {
	return rowjournal.WriteRequest{
		Target:      rowjournal.RowTarget(key),
		Op:          rowjournal.WriteUpdate,
		KeysChanged: true,
	}
}

// lockRowForDelete takes the row lock that deleting |sqlRow| requires and records the delete in the row journal.
func (w *prollyTableWriter) lockRowForDelete(ctx *sql.Context, sqlRow sql.Row) error {
	locks, ok := w.rowLocks(ctx)
	if !ok {
		return nil
	}

	key, err := w.lockTarget.keyer.rowLockKey(ctx, sqlRow)
	if err != nil {
		return err
	}

	return w.writeLockedRow(ctx, locks, rowjournal.WriteRequest{
		Target: rowjournal.RowTarget(key),
		Op:     rowjournal.WriteDelete,
	})
}

// writeLockedRow records a write to a row of this writer's table in the row journal, first waiting for the lock it
// requires. It returns an error if the lock can't be had, or if a transaction that this one cannot see has committed
// a change to the row.
func (w *prollyTableWriter) writeLockedRow(ctx *sql.Context, locks dsess.RowLocks, req rowjournal.WriteRequest) error {
	req.Relation = w.lockTarget.relation
	req.Compare = w.lockTarget.compare
	req.KeyFormat = w.lockTarget.keyFormat
	req.Timeout = dsess.RowLockWaitTimeout(ctx)

	res, err := locks.Journal.Write(ctx, locks.Txn, req)
	if err != nil {
		return dsess.RowLockWaitError(err, w.tblName.Name)
	}

	return dsess.RowLockConflictError(res)
}

// RowLockKeys describes the keys that identify a table's rows in the row journal, which are the rows' keys in the
// table's primary index. A reader that locks the rows it returns uses it to lock them by the same keys, compared the
// same way, that the table's writer uses for the rows it changes.
type RowLockKeys struct {
	// Compare orders the keys.
	Compare rowjournal.KeyCompare
	// KeyFormat identifies the format of the keys, which changes with the table's schema.
	KeyFormat hash.Hash
}

// NewRowLockKeys returns the RowLockKeys for |tbl|, which is the table named |tblName| in the database |dbName|.
func NewRowLockKeys(ctx *sql.Context, tbl *doltdb.Table, tblName doltdb.TableName, dbName string) (*RowLockKeys, error) {
	schState, err := writerSchema(ctx, tbl, tblName, dbName)
	if err != nil {
		return nil, err
	}

	keyFormat, err := tbl.GetSchemaHash(ctx)
	if err != nil {
		return nil, err
	}

	var keyer rowLockKeyer
	if schema.IsKeyless(schState.DoltSchema) {
		keyer, err = getPrimaryKeylessProllyWriter(ctx, tbl, schState)
	} else {
		keyer, err = getPrimaryProllyWriter(ctx, tbl, schState)
	}
	if err != nil {
		return nil, err
	}

	return &RowLockKeys{
		Compare:   keyer.rowLockCompare(),
		KeyFormat: keyFormat,
	}, nil
}
