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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// prollyWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type prollyWriteSession struct {
	workingSet *doltdb.WorkingSet
	tables     map[string]*prollyTableWriter
	aiTracker  globalstate.AutoIncrementTracker
	mut        *sync.RWMutex
}

var _ WriteSession = &prollyWriteSession{}

// GetTableWriter implemented WriteSession.
func (s *prollyWriteSession) GetTableWriter(ctx *sql.Context, table, db string, setter SessionRootSetter) (TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if tw, ok := s.tables[table]; ok {
		return tw, nil
	}

	t, ok, err := s.workingSet.WorkingRoot().GetTable(ctx, doltdb.TableName{Name: table})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	pkSch, err := sqlutil.FromDoltSchema("", table, sch)
	if err != nil {
		return nil, err
	}
	autoCol := autoIncrementColFromSchema(sch)

	var pw indexWriter
	var sws map[string]indexWriter
	if schema.IsKeyless(sch) {
		pw, err = getPrimaryKeylessProllyWriter(ctx, t, pkSch.Schema, sch)
		if err != nil {
			return nil, err
		}
		sws, err = getSecondaryKeylessProllyWriters(ctx, t, pkSch.Schema, sch, pw.(prollyKeylessWriter))
		if err != nil {
			return nil, err
		}
	} else {
		pw, err = getPrimaryProllyWriter(ctx, t, pkSch.Schema, sch)
		if err != nil {
			return nil, err
		}
		sws, err = getSecondaryProllyIndexWriters(ctx, t, pkSch.Schema, sch)
		if err != nil {
			return nil, err
		}
	}

	twr := &prollyTableWriter{
		tableName: table,
		dbName:    db,
		primary:   pw,
		secondary: sws,
		tbl:       t,
		sch:       sch,
		sqlSch:    pkSch.Schema,
		aiCol:     autoCol,
		aiTracker: s.aiTracker,
		flusher:   s,
		setter:    setter,
	}
	s.tables[table] = twr

	return twr, nil
}

// Flush implemented WriteSession.
func (s *prollyWriteSession) Flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx, false, nil)
}

func (s *prollyWriteSession) FlushWithAutoIncrementOverrides(ctx *sql.Context, autoIncSet bool, autoIncrements map[string]uint64) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx, autoIncSet, autoIncrements)
}

// SetWorkingSet implements WriteSession.
func (s *prollyWriteSession) SetWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.setWorkingSet(ctx, ws)
}

// GetOptions implemented WriteSession.
func (s *prollyWriteSession) GetOptions() editor.Options {
	return editor.Options{}
}

// SetOptions implemented WriteSession.
func (s *prollyWriteSession) SetOptions(opts editor.Options) {
	return
}

// flush is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flush(ctx *sql.Context, autoIncSet bool, manualAutoIncrementsSettings map[string]uint64) (*doltdb.WorkingSet, error) {
	tables := make(map[string]*doltdb.Table, len(s.tables))
	mu := &sync.Mutex{}

	eg, egCtx := errgroup.WithContext(ctx)
	sqlEgCtx := ctx.WithContext(egCtx)

	for n := range s.tables {
		name := n // make a copy
		eg.Go(func() error {
			wr := s.tables[name]
			t, err := wr.table(sqlEgCtx)
			if err != nil {
				return err
			}

			// Update this table's auto increment value if it has one. This value comes from the global state unless an
			// override was specified (e.g. if the next value was set explicitly)
			if schema.HasAutoIncrement(wr.sch) {
				autoIncVal := s.aiTracker.Current(name)
				override, hasManuallySetAi := manualAutoIncrementsSettings[name]
				if hasManuallySetAi {
					autoIncVal = override
				}

				// Update the table with the new auto-inc value if necessary. If it was set manually via an ALTER TABLE
				// statement, we defer to the tracker to update the value itself, since this impacts the global state.
				if hasManuallySetAi {
					t, err = s.aiTracker.Set(sqlEgCtx, name, t, s.workingSet.Ref(), autoIncVal)
					if err != nil {
						return err
					}
				} else if autoIncSet {
					t, err = t.SetAutoIncrementValue(sqlEgCtx, autoIncVal)
					if err != nil {
						return err
					}
				}
			}

			mu.Lock()
			defer mu.Unlock()
			tables[name] = t
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var err error
	flushed := s.workingSet.WorkingRoot()
	for name, tbl := range tables {
		flushed, err = flushed.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}

	s.workingSet = s.workingSet.WithWorkingRoot(flushed)

	return s.workingSet, nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (s *prollyWriteSession) setWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	root := ws.WorkingRoot()
	for tableName, tableWriter := range s.tables {
		t, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return err
		}
		if !ok { // table was removed in newer root
			delete(s.tables, tableName)
			continue
		}
		tSch, err := t.GetSchema(ctx)
		if err != nil {
			return err
		}

		err = tableWriter.Reset(ctx, s, t, tSch)
		if err != nil {
			return err
		}
	}
	s.workingSet = ws
	return nil
}
