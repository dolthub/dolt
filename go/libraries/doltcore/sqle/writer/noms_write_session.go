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

	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// WriteSession encapsulates writes made within a SQL session.
// It's responsible for creating and managing the lifecycle of TableWriter's.
type WriteSession interface {
	// GetTableWriter creates a TableWriter and adds it to the WriteSession.
	GetTableWriter(ctx context.Context, table, db string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error)

	// UpdateWorkingSet takes a callback to update this WriteSession's WorkingSet. The update method cannot change the
	// WorkingSetRef of the WriteSession. WriteSession flushes the pending writes in the session before calling the update.
	UpdateWorkingSet(ctx context.Context, cb func(ctx context.Context, current *doltdb.WorkingSet) (*doltdb.WorkingSet, error)) error

	// SetWorkingSet modifies the state of the WriteSession. The WorkingSetRef of |ws| must match the existing Ref.
	SetWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error

	// Flush flushes the pending writes in the session.
	Flush(ctx context.Context) (*doltdb.WorkingSet, error)

	// GetOptions returns the editor.Options for this session.
	GetOptions() editor.Options

	// SetOptions sets the editor.Options for this session.
	SetOptions(opts editor.Options)
}

// nomsWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type nomsWriteSession struct {
	opts editor.Options

	workingSet *doltdb.WorkingSet
	tables     map[string]*sessionedTableEditor
	writeMutex *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs
}

var _ WriteSession = &nomsWriteSession{}

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func NewWriteSession(nbf *types.NomsBinFormat, ws *doltdb.WorkingSet, opts editor.Options) WriteSession {
	if types.IsFormat_DOLT_1(nbf) {
		return &prollyWriteSession{
			workingSet: ws,
			tables:     make(map[string]*prollyTableWriter),
			mut:        &sync.RWMutex{},
		}
	}

	return &nomsWriteSession{
		opts:       opts,
		workingSet: ws,
		tables:     make(map[string]*sessionedTableEditor),
		writeMutex: &sync.RWMutex{},
	}
}

func (s *nomsWriteSession) GetTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

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

	te, err := s.getTableEditor(ctx, table, sch)
	if err != nil {
		return nil, err
	}

	conv := index.NewKVToSqlRowConverterForCols(t.Format(), sch)
	ac := autoIncrementColFromSchema(sch)

	return &nomsTableWriter{
		tableName:   table,
		dbName:      database,
		sch:         sch,
		autoIncCol:  ac,
		vrw:         vrw,
		kvToSQLRow:  conv,
		tableEditor: te,
		sess:        s,
		batched:     batched,
		aiTracker:   ait,
		setter:      setter,
	}, nil
}

// Flush returns an updated root with all of the changed tables.
func (s *nomsWriteSession) Flush(ctx context.Context) (*doltdb.WorkingSet, error) {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	return s.flush(ctx)
}

// SetWorkingSet implements WriteSession.
func (s *nomsWriteSession) SetWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()
	return s.setWorkingSet(ctx, ws)
}

// UpdateWorkingSet implements WriteSession.
func (s *nomsWriteSession) UpdateWorkingSet(ctx context.Context, cb func(ctx context.Context, current *doltdb.WorkingSet) (*doltdb.WorkingSet, error)) error {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

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
	rootMutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(len(s.tables))

	newRoot := s.workingSet.WorkingRoot()
	var tableErr error
	var rootErr error
	for tableName, ste := range s.tables {
		if !ste.HasEdits() {
			wg.Done()
			continue
		}

		// we can run all of the Table calls concurrently as long as we guard updating the root
		go func(tableName string, ste *sessionedTableEditor) {
			defer wg.Done()
			updatedTable, err := ste.tableEditor.Table(ctx)
			// we lock immediately after doing the operation, since both error setting and root updating are guarded
			rootMutex.Lock()
			defer rootMutex.Unlock()
			if err != nil {
				if tableErr == nil {
					tableErr = err
				}
				return
			}
			newRoot, err = newRoot.PutTable(ctx, tableName, updatedTable)
			if err != nil && rootErr == nil {
				rootErr = err
			}
		}(tableName, ste)
	}
	wg.Wait()
	if tableErr != nil {
		return nil, tableErr
	}
	if rootErr != nil {
		return nil, rootErr
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
		// Any existing references to this localTableEditor should be preserved, so we just change the underlying values
		localTableEditor.referencedTables = nil
		localTableEditor.referencingTables = nil
	} else {
		localTableEditor = &sessionedTableEditor{
			tableEditSession:  s,
			tableEditor:       nil,
			referencedTables:  nil,
			referencingTables: nil,
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

	if s.opts.ForeignKeyChecksDisabled {
		return localTableEditor, nil
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
	err = s.loadForeignKeys(ctx, localTableEditor)
	if err != nil {
		return nil, err
	}

	return localTableEditor, nil
}

// loadForeignKeys loads all tables mentioned in foreign keys for the given editor
func (s *nomsWriteSession) loadForeignKeys(ctx context.Context, localTableEditor *sessionedTableEditor) error {
	// these are the tables that reference us, so we need to update them
	for _, foreignKey := range localTableEditor.referencingTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := s.getTableEditor(ctx, foreignKey.TableName, nil)
		if err != nil {
			return err
		}
	}
	// these are the tables that we reference, so we need to refer to them
	for _, foreignKey := range localTableEditor.referencedTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := s.getTableEditor(ctx, foreignKey.ReferencedTableName, nil)
		if err != nil {
			return err
		}
	}
	return nil
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
	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
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
		localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
		if !s.opts.ForeignKeyChecksDisabled {
			err = s.loadForeignKeys(ctx, localTableEditor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
