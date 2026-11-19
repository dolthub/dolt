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

package rowjournal

import (
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

// RelationKey identifies a table on a branch.
type RelationKey struct {
	// Database is the base name of the database, never qualified with a revision.
	Database string
	// Branch is the working set ref of the branch, e.g. "workingSets/heads/main". Rows are locked per branch: the
	// same row on two branches is two different lock targets.
	Branch string
	// Table is the name of the table.
	Table doltdb.TableName
}

// Target identifies what is locked or written within a relation: one row, or the table as a whole.
type Target struct {
	// WholeTable is whether the target is the table itself rather than a row.
	WholeTable bool
	// Key identifies the row when WholeTable is false. For a table with a primary key this is the encoded key tuple;
	// for a keyless table it is the digest of the row's contents, which identifies all rows with those contents.
	Key []byte
}

// RowTarget returns the Target for the row with the key given.
func RowTarget(key []byte) Target {
	return Target{Key: key}
}

// TableTarget returns the Target for a table as a whole.
func TableTarget() Target {
	return Target{WholeTable: true}
}

// WriteOp is the kind of change a write makes to a row.
type WriteOp uint8

const (
	// WriteInsert creates a row.
	WriteInsert WriteOp = iota
	// WriteUpdate changes a row.
	WriteUpdate
	// WriteDelete removes a row.
	WriteDelete
)

// RefUpdateKind is the kind of change a version control operation makes to a ref.
type RefUpdateKind uint8

const (
	// RefCreate creates a ref.
	RefCreate RefUpdateKind = iota
	// RefDelete deletes a ref.
	RefDelete
	// RefRename renames a ref.
	RefRename
	// RefAdvance moves a ref to a descendant of its previous value, as a commit or a fast-forward merge does.
	RefAdvance
	// RefReset moves a ref to an arbitrary value, as a reset or a rebase does.
	RefReset
)

// SchemaChangeKind is the kind of change a DDL statement makes to a table.
type SchemaChangeKind uint8

const (
	// SchemaCreateTable creates a table.
	SchemaCreateTable SchemaChangeKind = iota
	// SchemaAlterTable changes the definition of a table.
	SchemaAlterTable
	// SchemaDropTable drops a table.
	SchemaDropTable
	// SchemaRenameTable renames a table.
	SchemaRenameTable
)

// Record is one entry in the journal. The state of a Journal is the result of applying every Record appended to it,
// in order. Records are values: they refer to transactions, tables and rows by identifier so that they can be
// serialized.
type Record interface {
	// TxnID returns the transaction the record belongs to, or zero when it belongs to none.
	TxnID() TxnID
}

// TxnBeginRecord registers a transaction.
type TxnBeginRecord struct {
	Txn       TxnID
	SessionID uint32
	StartTime time.Time
	Snapshot  Snapshot
}

// TxnCommitRecord marks a transaction as committed and releases its locks.
type TxnCommitRecord struct {
	Txn TxnID
}

// TxnAbortRecord marks a transaction as aborted, discards its writes and releases its locks.
type TxnAbortRecord struct {
	Txn TxnID
}

// SavepointBeginRecord opens a savepoint. Level is the new savepoint level of the transaction.
type SavepointBeginRecord struct {
	Txn   TxnID
	Level uint32
}

// SavepointReleaseRecord folds everything done at Level or deeper into the level below.
type SavepointReleaseRecord struct {
	Txn   TxnID
	Level uint32
}

// SavepointRollbackRecord discards the writes and releases the locks taken at Level or deeper.
type SavepointRollbackRecord struct {
	Txn   TxnID
	Level uint32
}

// RowLockRecord grants a lock.
type RowLockRecord struct {
	Txn      TxnID
	Level    uint32
	Relation RelationKey
	Target   Target
	Mode     LockMode
}

// RowWriteRecord records a change to a row, or with a table target, to an unspecified set of rows in a table.
type RowWriteRecord struct {
	Txn      TxnID
	Level    uint32
	Relation RelationKey
	Target   Target
	// Statement numbers the statement that made the write within its transaction.
	Statement uint32
	Op        WriteOp
	// KeysChanged is whether an update changed any column of the row's key.
	KeysChanged bool
	// Successor is the row's new key when an update changed it.
	Successor []byte
	// Value is the row's non-key columns after the write. Not populated yet: it is reserved for isolation levels that
	// read the latest committed version of a row.
	Value []byte
}

// RefUpdateRecord records a version control operation's change to a ref. Nothing consumes these records yet.
type RefUpdateRecord struct {
	Txn      TxnID
	Database string
	Kind     RefUpdateKind
	// Ref is the full name of the ref, e.g. "refs/heads/main".
	Ref string
	// NewRef is the ref's new name, for a rename.
	NewRef  string
	OldHash hash.Hash
	NewHash hash.Hash
}

// SchemaChangeRecord records a DDL statement's change to a table. Nothing consumes these records yet.
type SchemaChangeRecord struct {
	Txn      TxnID
	Relation RelationKey
	Kind     SchemaChangeKind
	// NewTable is the table's new name, for a rename.
	NewTable doltdb.TableName
}

// KeyFormatChangeRecord records that the rows of a relation are now identified by keys of a different format, as
// they are after its table's schema changes or the table is dropped and created again. Keys of the old format can't be
// compared with keys of the new one, so the locks and writes recorded against rows become locks and writes on the
// table as a whole, which is the safe way to lose track of which rows they were for.
type KeyFormatChangeRecord struct {
	Relation  RelationKey
	KeyFormat hash.Hash
}

// PruneRecord discards the writes of every committed transaction with an ID below Horizon. Those transactions are
// visible to every snapshot that exists or can be taken, so their writes can no longer conflict with anything.
type PruneRecord struct {
	Horizon TxnID
}

func (r TxnBeginRecord) TxnID() TxnID          { return r.Txn }
func (r TxnCommitRecord) TxnID() TxnID         { return r.Txn }
func (r TxnAbortRecord) TxnID() TxnID          { return r.Txn }
func (r SavepointBeginRecord) TxnID() TxnID    { return r.Txn }
func (r SavepointReleaseRecord) TxnID() TxnID  { return r.Txn }
func (r SavepointRollbackRecord) TxnID() TxnID { return r.Txn }
func (r RowLockRecord) TxnID() TxnID           { return r.Txn }
func (r RowWriteRecord) TxnID() TxnID          { return r.Txn }
func (r RefUpdateRecord) TxnID() TxnID         { return r.Txn }
func (r SchemaChangeRecord) TxnID() TxnID      { return r.Txn }
func (r KeyFormatChangeRecord) TxnID() TxnID   { return 0 }
func (r PruneRecord) TxnID() TxnID             { return 0 }

// RecordSink receives every record appended to a Journal, in order. Append is called with the journal's internal lock
// held, so it must not block and must not call back into the Journal.
type RecordSink interface {
	Append(Record)
}
