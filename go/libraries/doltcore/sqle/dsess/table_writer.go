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
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// WriteSession encapsulates writes made within a SQL session.
// It's responsible for creating and managing the lifecycle of TableWriter's.
type WriteSession interface {
	// GetTableWriter creates a TableWriter and adds it to the WriteSession.
	GetTableWriter(ctx *sql.Context, table doltdb.TableName, db string, setter SessionRootSetter, targetStaging bool) (TableWriter, error)

	// GetWorkingSet returns the session's current working set.
	GetWorkingSet() *doltdb.WorkingSet

	// SetWorkingSet modifies the state of the WriteSession. The WorkingSetRef of |ws| must match the existing Ref.
	SetWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error

	// VisitGCRoots is used to ensure that a write session's GC roots are retained
	// in the case that a GC needs to proceed before the write session is flushed.
	VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error

	// GetOptions returns the editor.Options for this session.
	GetOptions() editor.Options

	// SetOptions sets the editor.Options for this session.
	SetOptions(opts editor.Options)

	WriteSessionFlusher
}

type TableWriter interface {
	sql.TableEditor
	sql.ForeignKeyEditor
	sql.AutoIncrementSetter
}

// SessionRootSetter sets the root value for the session.
type SessionRootSetter func(ctx *sql.Context, dbName string, root doltdb.RootValue) error

// WriteSessionFlusher is responsible for flushing any pending edits to the session
type WriteSessionFlusher interface {
	// Flush flushes the pending writes in the session.
	Flush(ctx *sql.Context) (*doltdb.WorkingSet, error)
	// FlushWithAutoIncrementOverrides flushes the pending writes in the session, overriding the auto increment values
	// for any tables provided in the map
	FlushWithAutoIncrementOverrides(ctx *sql.Context, increment bool, autoIncrements map[string]uint64) (*doltdb.WorkingSet, error)
}

// WriterState caches expensive objects required for writing rows.
// All objects in writerState are valid as long as a table schema
// is the same.
type WriterState struct {
	DoltSchema schema.Schema
	AutoIncCol schema.Column
	PkKeyDesc  val.TupleDesc
	PkValDesc  val.TupleDesc
	PkSchema   sql.PrimaryKeySchema
	SecIndexes []IndexState
	PriIndex   IndexState
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
}
