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

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// nomsWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type nomsWriteSession struct {
	workingSet *doltdb.WorkingSet
	tables     map[string]*sessionedTableEditor
	aiTracker  globalstate.AutoIncrementTracker
	mut        *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs
	opts       editor.Options
}

var _ dsess.WriteSession = &nomsWriteSession{}

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetWorkingRoot before any
// table editors are returned.
func NewWriteSession(nbf *types.NomsBinFormat, ws *doltdb.WorkingSet, aiTracker globalstate.AutoIncrementTracker, opts editor.Options) dsess.WriteSession {
	if types.IsFormat_DOLT(nbf) {
		return &prollyWriteSession{
			workingSet:    ws,
			tables:        make(map[doltdb.TableName]*prollyTableWriter),
			aiTracker:     aiTracker,
			mut:           &sync.RWMutex{},
			targetStaging: opts.TargetStaging,
		}
	}

	return &nomsWriteSession{
		workingSet: ws,
		tables:     make(map[string]*sessionedTableEditor),
		aiTracker:  aiTracker,
		mut:        &sync.RWMutex{},
		opts:       opts,
	}
}

func (s *nomsWriteSession) GetWorkingSet() *doltdb.WorkingSet {
	return s.workingSet
}

func (s *nomsWriteSession) GetTableWriter(ctx *sql.Context, table doltdb.TableName, db string, setter dsess.SessionRootSetter, targetStaging bool) (dsess.TableWriter, error) {
	if targetStaging {
		// This would be fairly easy to implement, but we gotta stop luggin around the legacy storage format.
		return nil, fmt.Errorf("Feature not supported in legacy storage format")
	}

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
	sqlSch, err := sqlutil.FromDoltSchema("", table.Name, sch)
	if err != nil {
		return nil, err
	}

	te, err := s.getTableEditor(ctx, table.Name, sch)
	if err != nil {
		return nil, err
	}

	conv := index.NewKVToSqlRowConverterForCols(t.Format(), sch, nil)

	return &nomsTableWriter{
		tableName:   table.Name,
		dbName:      db,
		sch:         sch,
		sqlSch:      sqlSch.Schema,
		vrw:         vrw,
		kvToSQLRow:  conv,
		tableEditor: te,
		flusher:     s,
		autoInc:     s.aiTracker,
		setter:      setter,
	}, nil
}

// Flush returns an updated root with all of the changed tables.
func (s *nomsWriteSession) Flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx)
}

func (s *nomsWriteSession) FlushWithAutoIncrementOverrides(ctx *sql.Context, increment bool, autoIncrements map[string]uint64) (*doltdb.WorkingSet, error) {
	// auto increment overrides not implemented
	return s.Flush(ctx)
}

// SetWorkingSet implements WriteSession.
func (s *nomsWriteSession) SetWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.setWorkingSet(ctx, ws)
}

func (s *nomsWriteSession) GetOptions() editor.Options {
	return s.opts
}

func (s *nomsWriteSession) SetOptions(opts editor.Options) {
	s.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (s *nomsWriteSession) flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	newRoot := s.workingSet.WorkingRoot()
	mu := &sync.Mutex{}
	rootUpdate := func(name string, table *doltdb.Table) (err error) {
		mu.Lock()
		defer mu.Unlock()
		if newRoot != nil {
			newRoot, err = newRoot.PutTable(ctx, doltdb.TableName{Name: name}, table)
		}
		return err
	}

	eg, egCtx := errgroup.WithContext(ctx)
	ctx = ctx.WithContext(egCtx)

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
				v, err := s.aiTracker.Current(name)
				if err != nil {
					return err
				}
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

	t, ok, err = root.GetTable(ctx, doltdb.TableName{Name: tableName})
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

// setRoot is the inner implementation for SetWorkingRoot that does not acquire any locks
func (s *nomsWriteSession) setWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	if ws == nil {
		return fmt.Errorf("cannot set a nomsWriteSession's working set to nil once it has been created")
	}
	if s.workingSet != nil && s.workingSet.Ref() != ws.Ref() {
		return fmt.Errorf("cannot change working set ref using SetWorkingSet")
	}
	s.workingSet = ws

	root := ws.WorkingRoot()
	for tableName, localTableEditor := range s.tables {
		t, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
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
