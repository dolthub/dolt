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
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// WriteSession encapsulates writes made within a SQL session.
// It's responsible for creating and managing the lifecycle of TableWriter's.
type WriteSession interface {
	// GetTableWriter creates a TableWriter and adds it to the WriteSession.
	GetTableWriter(ctx context.Context, table, db string, setter SessionRootSetter, batched bool) (TableWriter, error)

	// UpdateWorkingSet takes a callback to update this WriteSession's WorkingSet. The update method cannot change the
	// WorkingSetRef of the WriteSession. WriteSession flushes the pending writes in the session before calling the update.
	UpdateWorkingSet(ctx context.Context, cb func(ctx context.Context, current *doltdb.WorkingSet) (*doltdb.WorkingSet, error)) error

	// SetWorkingSet modifies the state of the WriteSession. The WorkingSetRef of |ws| must match the existing Ref.
	SetWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error

	// GetOptions returns the editor.Options for this session.
	GetOptions() editor.Options

	// SetOptions sets the editor.Options for this session.
	SetOptions(opts editor.Options)

	WriteSessionFlusher
}

// WriteSessionFlusher is responsible for flushing any pending edits to the session
type WriteSessionFlusher interface {
	// Flush flushes the pending writes in the session.
	Flush(ctx context.Context) (*doltdb.WorkingSet, error)
}

// nomsWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type nomsWriteSession struct {
	workingSet *doltdb.WorkingSet
	tables     map[string]*sessionedTableEditor
	tracker    globalstate.AutoIncrementTracker
	mut        *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs
	opts       editor.Options
}

var _ WriteSession = &nomsWriteSession{}

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func NewWriteSession(nbf *types.NomsBinFormat, ws *doltdb.WorkingSet, tracker globalstate.AutoIncrementTracker, opts editor.Options) WriteSession {
	if types.IsFormat_DOLT_1(nbf) {
		return &prollyWriteSession{
			workingSet: ws,
			tables:     make(map[string]*prollyTableWriter),
			tracker:    tracker,
			mut:        &sync.RWMutex{},
		}
	}

	return &nomsWriteSession{
		workingSet: ws,
		tables:     make(map[string]*sessionedTableEditor),
		tracker:    tracker,
		mut:        &sync.RWMutex{},
		opts:       opts,
	}
}

func (s *nomsWriteSession) GetTableWriter(ctx context.Context, table, db string, setter SessionRootSetter, batched bool) (TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	t, ok, err := s.workingSet.WorkingRoot().GetTable(ctx, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}
	vrw := t.ValueReadWriter()

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema(table, sch)
	if err != nil {
		return nil, err
	}

	te, err := s.getTableEditor(ctx, table, sch)
	if err != nil {
		return nil, err
	}

	conv := index.NewKVToSqlRowConverterForCols(t.Format(), sch)

	return &nomsTableWriter{
		tableName:   table,
		dbName:      db,
		sch:         sch,
		sqlSch:      sqlSch.Schema,
		vrw:         vrw,
		kvToSQLRow:  conv,
		tableEditor: te,
		flusher:     s,
		batched:     batched,
		autoInc:     s.tracker,
		setter:      setter,
	}, nil
}

// Flush returns an updated root with all of the changed tables.
func (s *nomsWriteSession) Flush(ctx context.Context) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx)
}

// SetWorkingSet implements WriteSession.
func (s *nomsWriteSession) SetWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.setWorkingSet(ctx, ws)
}

// UpdateWorkingSet implements WriteSession.
func (s *nomsWriteSession) UpdateWorkingSet(ctx context.Context, cb func(ctx context.Context, current *doltdb.WorkingSet) (*doltdb.WorkingSet, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	current, err := s.flush(ctx)
	if err != nil {
		return err
	}

	mutated, err := cb(ctx, current)
	if err != nil {
		return err
	}
	s.workingSet = mutated

	return s.setWorkingSet(ctx, s.workingSet)
}

func (s *nomsWriteSession) GetOptions() editor.Options {
	return s.opts
}

func (s *nomsWriteSession) SetOptions(opts editor.Options) {
	s.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (s *nomsWriteSession) flush(ctx context.Context) (*doltdb.WorkingSet, error) {
	newRoot := s.workingSet.WorkingRoot()
	mu := &sync.Mutex{}
	rootUpdate := func(name string, table *doltdb.Table) (err error) {
		mu.Lock()
		defer mu.Unlock()
		if newRoot != nil {
			newRoot, err = newRoot.PutTable(ctx, name, table)
		}
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)

	for tblName, tblEditor := range s.tables {
		if !tblEditor.HasEdits() {
			continue
		}

		// copy variables
		name, ed := tblName, tblEditor

		eg.Go(func() error {
			tbl, err := ed.tableEditor.Table(ctx)
			if err != nil {
				return err
			}

			// Update the auto increment value for the table if a tracker was provided
			// TODO: the table probably needs an autoincrement tracker no matter what
			if schema.HasAutoIncrement(ed.Schema()) {
				v := s.tracker.Current(name)
				tbl, err = tbl.SetAutoIncrementValue(ctx, v)
				if err != nil {
					return err
				}
			}

			return rootUpdate(name, tbl)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	s.workingSet = s.workingSet.WithWorkingRoot(newRoot)

	return s.workingSet, nil
}

// getTableEditor is the inner implementation for GetTableEditor, allowing recursive calls
func (s *nomsWriteSession) getTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (*sessionedTableEditor, error) {
	if s.workingSet == nil {
		return nil, fmt.Errorf("must call SetWorkingSet before a table editor will be returned")
	}

	var t *doltdb.Table
	var err error
	localTableEditor, ok := s.tables[tableName]
	if ok {
		if tableSch == nil {
			return localTableEditor, nil
		} else if schema.SchemasAreEqual(tableSch, localTableEditor.tableEditor.Schema()) {
			return localTableEditor, nil
		}
	} else {
		localTableEditor = &sessionedTableEditor{
			tableEditSession: s,
			tableEditor:      nil,
		}
		s.tables[tableName] = localTableEditor
	}

	root := s.workingSet.WorkingRoot()

	t, ok, err = root.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unable to create table editor as `%s` is missing", tableName)
	}
	if tableSch == nil {
		tableSch, err = t.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	tableEditor, err := editor.NewTableEditor(ctx, t, tableSch, tableName, s.opts)
	if err != nil {
		return nil, err
	}

	localTableEditor.tableEditor = tableEditor

	return localTableEditor, nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (s *nomsWriteSession) setWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	if ws == nil {
		return fmt.Errorf("cannot set a nomsWriteSession's working set to nil once it has been created")
	}
	if s.workingSet != nil && s.workingSet.Ref() != ws.Ref() {
		return fmt.Errorf("cannot change working set ref using SetWorkingSet")
	}
	s.workingSet = ws

	root := ws.WorkingRoot()
	if err := s.updateAutoIncrementSequences(ctx, root); err != nil {
		return err
	}

	for tableName, localTableEditor := range s.tables {
		t, ok, err := root.GetTable(ctx, tableName)
		if err != nil {
			return err
		}
		if !ok { // table was removed in newer root
			if err := localTableEditor.tableEditor.Close(ctx); err != nil {
				return err
			}
			delete(s.tables, tableName)
			continue
		}
		tSch, err := t.GetSchema(ctx)
		if err != nil {
			return err
		}

		newTableEditor, err := editor.NewTableEditor(ctx, t, tSch, tableName, s.opts)
		if err != nil {
			return err
		}
		if err := localTableEditor.tableEditor.Close(ctx); err != nil {
			return err
		}
		localTableEditor.tableEditor = newTableEditor
	}
	return nil
}

func (s *nomsWriteSession) updateAutoIncrementSequences(ctx context.Context, root *doltdb.RootValue) error {
	return root.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if !schema.HasAutoIncrement(sch) {
			return
		}
		v, err := table.GetAutoIncrementValue(ctx)
		if err != nil {
			return true, err
		}
		s.tracker.Set(name, v)
		return
	})
}
