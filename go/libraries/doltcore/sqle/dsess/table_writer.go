// Copyright 2024 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// WriteSession encapsulates writes made within a SQL session.
// It's responsible for creating and managing the lifecycle of TableWriter's.
type WriteSession interface {
	// GetTableWriter creates a TableWriter and adds it to the WriteSession.
	GetTableWriter(ctx *sql.Context, table doltdb.TableName) (TableWriter, error)

	// GetTableWriterForNewTable is GetTableWriter for |tbl|, a table that isn't in the owner's working set yet. The
	// table is added to it when the writer flushes.
	GetTableWriterForNewTable(ctx *sql.Context, table doltdb.TableName, tbl *doltdb.Table) (TableWriter, error)

	// GetWorkingSet returns its owner's current working set.
	GetWorkingSet() *doltdb.WorkingSet

	// ResetTableWriters rebuilds this session's table writers from its owner's working set, discarding their unflushed
	// edits. The owner calls it when its working set is replaced other than through this session's own flushes.
	ResetTableWriters(ctx *sql.Context) error

	// VisitGCRoots is used to ensure that a write session's GC roots are retained
	// in the case that a GC needs to proceed before the write session is flushed.
	VisitGCRoots(ctx context.Context, roots func(hash.Hash)) error

	// GetOptions returns the editor.Options for this session.
	GetOptions() editor.Options

	// HasOpenTableWriters returns whether any TableWriter this session has handed out hasn't been closed since.
	HasOpenTableWriters() bool

	// SetDeferMaterialization controls whether the edits of completed statements are materialized into the working
	// set as each statement completes. When deferred, edits accumulate in the session's TableWriters until
	// MaterializePending is called, and the session's owner must do so before reading the working set.
	SetDeferMaterialization(deferMaterialization bool)

	// HasPendingEdits returns true if edits from completed statements have not been materialized into the working set.
	HasPendingEdits() bool

	// PendingTables returns the names of tables with edits that have not been materialized into the working set.
	PendingTables() []doltdb.TableName

	// PendingRows returns views of the rows of |tblName| that include its pending edits. It returns false if the
	// table has no pending edits, and nil views if it has pending edits that can't be read without materializing
	// them.
	PendingRows(ctx *sql.Context, tblName doltdb.TableName) (*index.PendingRows, bool, error)

	// MaterializePending materializes all pending edits into its owner's working set. The edits of a statement in
	// progress are not included.
	MaterializePending(ctx *sql.Context) error

	WriteSessionFlusher
}

type TableWriter interface {
	sql.TableEditor
	sql.ForeignKeyEditor
	sql.AutoIncrementSetter
}

// SessionRootSetter sets the root value for the session.
type SessionRootSetter func(ctx *sql.Context, dbName string, root doltdb.RootValue) error

// WorkingSetOwner holds the working set that a WriteSession reads its tables from and flushes them into.
type WorkingSetOwner interface {
	// WorkingSet returns the owner's current working set.
	WorkingSet() *doltdb.WorkingSet
	// SetFlushedWorkingSet replaces the owner's working set with |ws|, which its WriteSession built by flushing table
	// writers into the current one.
	SetFlushedWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error
}

// WriteSessionFlusher is responsible for flushing any pending edits to the session
type WriteSessionFlusher interface {
	// Flush flushes the pending writes in the session
	Flush(ctx *sql.Context) (*doltdb.WorkingSet, error)
	// FlushTable writes a materialized doltdb.Table to the RootValue
	FlushTable(ctx *sql.Context, tblName doltdb.TableName, tbl *doltdb.Table) (doltdb.RootValue, error)
}

// WriterState caches expensive objects required for writing rows.
// All objects in writerState are valid as long as a table schema
// is the same.
type WriterState struct {
	DoltSchema schema.Schema
	AutoIncCol schema.Column
	PkSchema   sql.PrimaryKeySchema
	PkKeyDesc  *val.TupleDesc
	PkValDesc  *val.TupleDesc
	SecIndexes []IndexState
	PriIndex   IndexState
	// VirtualExpressions holds the table's resolved virtual column expressions, indexed by column
	// position in the full schema.
	VirtualExpressions []sql.Expression
}

// IndexState caches objects required for writing specific indexes.
// The objects are valid as long as the index's schema is the same.
type IndexState struct {
	Schema        schema.Schema
	Name          string
	ValMapping    val.OrdinalMapping
	KeyMapping    val.OrdinalMapping
	PkMapping     val.OrdinalMapping
	PrefixLengths []uint16
	Count         int
	IsFullText    bool
	IsUnique      bool
	IsSpatial     bool
	Predicate     sql.Expression
	// KeyVirtualExprs is parallel to KeyMapping. A non-nil entry at position i is the resolved
	// generating expression for a virtual generated column that is key part i of this index.
	KeyVirtualExprs []sql.Expression
}
