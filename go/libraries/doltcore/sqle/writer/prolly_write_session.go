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
	tracker    globalstate.AutoIncrementTracker
	mut        *sync.RWMutex
}

var _ WriteSession = &prollyWriteSession{}

// GetTableWriter implemented WriteSession.
func (s *prollyWriteSession) GetTableWriter(ctx context.Context, table, db string, setter SessionRootSetter, batched bool) (TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if tw, ok := s.tables[table]; ok {
		return tw, nil
	}

	t, ok, err := s.workingSet.WorkingRoot().GetTable(ctx, table)
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
	pkSch, err := sqlutil.FromDoltSchema(table, sch)
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
		aiTracker: s.tracker,
		flusher:   s,
		setter:    setter,
		batched:   batched,
	}
	s.tables[table] = twr

	return twr, nil
}

// Flush implemented WriteSession.
func (s *prollyWriteSession) Flush(ctx context.Context) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx)
}

// SetWorkingSet implements WriteSession.
func (s *prollyWriteSession) SetWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.setWorkingSet(ctx, ws)
}

// UpdateWorkingSet implements WriteSession.
func (s *prollyWriteSession) UpdateWorkingSet(ctx context.Context, cb func(ctx context.Context, current *doltdb.WorkingSet) (*doltdb.WorkingSet, error)) error {
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

	return s.SetWorkingSet(ctx, mutated)
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
func (s *prollyWriteSession) flush(ctx context.Context) (*doltdb.WorkingSet, error) {
	var err error
	tables := make(map[string]*doltdb.Table, len(s.tables))
	mu := &sync.Mutex{}

	eg, ctx := errgroup.WithContext(ctx)
	for n := range s.tables {
		name := n // make a copy
		eg.Go(func() error {
			wr := s.tables[name]
			t, err := wr.table(ctx)
			if err != nil {
				return err
			}

			if schema.HasAutoIncrement(wr.sch) {
				v := s.tracker.Current(name)
				t, err = t.SetAutoIncrementValue(ctx, v)
				if err != nil {
					return err
				}
			}

			mu.Lock()
			defer mu.Unlock()
			tables[name] = t
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

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
		t, ok, err := root.GetTable(ctx, tableName)
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
