// Copyright 2020 Dolthub, Inc.
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
	"context"

	"github.com/dolthub/go-mysql-server/errguard"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

// NewWriteSession returns a WriteSession that reads tables from |owner|'s working set and flushes them into it.
func NewWriteSession(dbName string, owner dsess.WorkingSetOwner, aiTracker *dsess.AutoIncrementTracker, opts editor.Options) dsess.WriteSession {
	return &prollyWriteSession{
		dbName:        dbName,
		tables:        make(map[doltdb.TableName]*prollyTableWriter),
		aiTracker:     aiTracker,
		owner:         owner,
		targetStaging: opts.TargetStaging,
	}
}

// NewDetachedWriteSession returns a WriteSession with its own copy of |ws|, for writes that don't go to a branch state,
// such as a temporary table's. Each flush updates the copy and passes its root to |setter|. A write session that
// starts ahead of a branch state uses DoltSession.ForkWorkingSet instead.
func NewDetachedWriteSession(dbName string, ws *doltdb.WorkingSet, aiTracker *dsess.AutoIncrementTracker, setter dsess.SessionRootSetter, opts editor.Options) dsess.WriteSession {
	owner := &detachedOwner{dbName: dbName, ws: ws, setter: setter, targetStaging: opts.TargetStaging}
	return NewWriteSession(dbName, owner, aiTracker, opts)
}

// detachedOwner is the WorkingSetOwner of a write session created by NewDetachedWriteSession.
type detachedOwner struct {
	dbName        string
	ws            *doltdb.WorkingSet
	setter        dsess.SessionRootSetter
	targetStaging bool
}

func (o *detachedOwner) WorkingSet() *doltdb.WorkingSet {
	return o.ws
}

func (o *detachedOwner) SetFlushedWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	o.ws = ws
	if o.targetStaging {
		return o.setter(ctx, o.dbName, ws.StagedRoot())
	}
	return o.setter(ctx, o.dbName, ws.WorkingRoot())
}

// prollyWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type prollyWriteSession struct {
	dbName        string
	tables        map[doltdb.TableName]*prollyTableWriter
	aiTracker     *dsess.AutoIncrementTracker
	owner         dsess.WorkingSetOwner
	targetStaging bool
}

var _ dsess.WriteSession = &prollyWriteSession{}

// GetWorkingSet implements WriteSession
func (s *prollyWriteSession) GetWorkingSet() *doltdb.WorkingSet {
	return s.owner.WorkingSet()
}

// root returns the root this session's table writers read from and flush into: the owner's working root, or its
// staged root under TargetStaging.
func (s *prollyWriteSession) root() doltdb.RootValue {
	if s.targetStaging {
		return s.owner.WorkingSet().StagedRoot()
	}
	return s.owner.WorkingSet().WorkingRoot()
}

// withRoot returns the owner's working set with |root| in place of the root this session writes to.
func (s *prollyWriteSession) withRoot(root doltdb.RootValue) *doltdb.WorkingSet {
	if s.targetStaging {
		return s.owner.WorkingSet().WithStagedRoot(root)
	}
	return s.owner.WorkingSet().WithWorkingRoot(root)
}

func (s *prollyWriteSession) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	for _, writer := range s.tables {
		err := writer.VisitGCRoots(ctx, roots)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetTableWriter implements WriteSession
func (s *prollyWriteSession) GetTableWriter(ctx *sql.Context, tblName doltdb.TableName) (dsess.TableWriter, error) {
	if tw, ok := s.tables[tblName]; ok {
		tw.open = true
		return tw, nil
	}

	// Read from the owner, not |ctx|'s session. fullTextRewriteEditor, for one, rebuilds full-text indexes from a
	// detached owner's working set while the session still has the old data.
	tbl, ok, err := s.root().GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}
	return s.newTableWriter(ctx, tblName, tbl)
}

// GetTableWriterForNewTable implements WriteSession
func (s *prollyWriteSession) GetTableWriterForNewTable(ctx *sql.Context, tblName doltdb.TableName, tbl *doltdb.Table) (dsess.TableWriter, error) {
	if tw, ok := s.tables[tblName]; ok {
		tw.open = true
		return tw, nil
	}
	return s.newTableWriter(ctx, tblName, tbl)
}

func (s *prollyWriteSession) newTableWriter(ctx *sql.Context, tblName doltdb.TableName, tbl *doltdb.Table) (dsess.TableWriter, error) {
	tblWriter := &prollyTableWriter{
		tblName:   tblName,
		dbName:    s.dbName,
		aiTracker: s.aiTracker,
		writeSess: s,
	}
	err := tblWriter.Reset(ctx, tbl)
	if err != nil {
		return nil, err
	}

	tblWriter.open = true
	s.tables[tblName] = tblWriter
	return tblWriter, nil
}

// HasOpenTableWriters implements WriteSession
func (s *prollyWriteSession) HasOpenTableWriters() bool {
	for _, tw := range s.tables {
		if tw.open {
			return true
		}
	}
	return false
}

// ResetTableWriters implements WriteSession
func (s *prollyWriteSession) ResetTableWriters(ctx *sql.Context) error {
	root := s.root()
	for tblName, tblWriter := range s.tables {
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		// table was removed in newer root
		if !ok {
			delete(s.tables, tblName)
			continue
		}
		err = tblWriter.Reset(ctx, tbl)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetOptions implements WriteSession
func (s *prollyWriteSession) GetOptions() editor.Options {
	return editor.Options{
		TargetStaging: s.targetStaging,
	}
}

// Flush implements WriteSessionFlusher. It returns the owner's working set with every table writer materialized into
// it, without passing that to the owner.
func (s *prollyWriteSession) Flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	flushed, err := s.flushAllTables(ctx)
	if err != nil {
		return nil, err
	}
	return s.withRoot(flushed), nil
}

// FlushTable puts the already materialized table into the owner's working set
func (s *prollyWriteSession) FlushTable(ctx *sql.Context, tblName doltdb.TableName, tbl *doltdb.Table) (doltdb.RootValue, error) {
	flushed, err := s.root().PutTable(ctx, tblName, tbl)
	if err != nil {
		return nil, err
	}
	err = s.owner.SetFlushedWorkingSet(ctx, s.withRoot(flushed))
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

// publish hands the owner's working set back to it, as FlushTable does without a table to put. A detached owner
// starts ahead of the session, as a table rewrite's does, and passes its root to the session only on a flush.
func (s *prollyWriteSession) publish(ctx *sql.Context) error {
	return s.owner.SetFlushedWorkingSet(ctx, s.owner.WorkingSet())
}

// flushAllTables is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flushAllTables(ctx *sql.Context) (doltdb.RootValue, error) {
	type flushedTable struct {
		tblName doltdb.TableName
		tbl     *doltdb.Table
	}

	// Materialize each table concurrently.
	flushedTables := make([]flushedTable, len(s.tables))
	eg, egCtx := ctx.NewErrgroup()
	i := 0
	for tblName, tblWriter := range s.tables {
		res := &flushedTables[i]
		i++
		errguard.Go(eg, func() error {
			tbl, err := tblWriter.table(egCtx)
			if err != nil {
				return err
			}
			*res = flushedTable{tblName: tblName, tbl: tbl}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	flushed := s.root()
	for _, ft := range flushedTables {
		var err error
		flushed, err = flushed.PutTable(ctx, ft.tblName, ft.tbl)
		if err != nil {
			return nil, err
		}
	}

	// Flush's callers decide where the result goes. The binlog applier writes it straight to the database, and passing
	// it to a branch state would make that state dirty, so CommitTransaction would refuse a replicated transaction that
	// spans databases.
	return flushed, nil
}
